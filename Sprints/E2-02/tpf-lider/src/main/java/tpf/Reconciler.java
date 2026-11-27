package tpf;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class Reconciler {

    private static final class PendingPosse {
        final String replicaId;
        final String chunkId;
        final String esperadoHex;

        PendingPosse(String replicaId, String chunkId, String esperadoHex) {
            this.replicaId = replicaId;
            this.chunkId = chunkId;
            this.esperadoHex = esperadoHex;
        }
    }

    private final Path sealedDir;
    private final ReplicaRegistry registry;
    private final Catalog catalog;
    private final Placement placement;
    private final ChunkCrypto crypto;
    private final PubSubHub hub;
    private final AtomicInteger currentR;
    private final BiConsumer<String, String> eventLogger;
    private final BiConsumer<String, String> fetchRequester;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final Map<String, PendingPosse> pendingPosses = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private long lastManifestPublishMs = 0L;
    private int manifestVersion = 0;

    public Reconciler(Path sealedDir, ReplicaRegistry registry, Catalog catalog, Placement placement,
                      ChunkCrypto crypto, PubSubHub hub, AtomicInteger currentR,
                      BiConsumer<String, String> eventLogger,
                      BiConsumer<String, String> fetchRequester) {
        this.sealedDir = sealedDir;
        this.registry = registry;
        this.catalog = catalog;
        this.placement = placement;
        this.crypto = crypto;
        this.hub = hub;
        this.currentR = currentR;
        this.eventLogger = eventLogger;
        this.fetchRequester = fetchRequester;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::runRoundSafe, Wire.RECONCILIACAO_MS, Wire.RECONCILIACAO_MS, TimeUnit.MILLISECONDS);
    }

    public void triggerNow() {
        scheduler.execute(this::runRoundSafe);
    }

    private void runRoundSafe() {
        try {
            runRound();
        } catch (Exception e) {
            eventLogger.accept("ERRO", "Erro no reconciliador: " + e.getMessage());
        }
    }

    public void runRound() throws Exception {
        long now = System.currentTimeMillis();
        int rTarget = currentR.get();

        // 1. Atualizar estado das réplicas (SUSPEITA / MORTA)
        for (ReplicaRegistry.ReplicaInfo rep : registry.all()) {
            long elapsed = now - rep.monitor.getLastArrivalMs();
            if (elapsed > Wire.MORTA_MS && rep.state != ReplicaRegistry.State.MORTA) {
                rep.state = ReplicaRegistry.State.MORTA;
                eventLogger.accept("MORTA", rep.id + " sem heartbeat há " + elapsed + "ms");
            } else if (elapsed > 3 * Wire.PERIODO_HB_MS && elapsed <= Wire.MORTA_MS
                    && rep.state == ReplicaRegistry.State.VIVA) {
                rep.state = ReplicaRegistry.State.SUSPEITA;
                eventLogger.accept("SUSPEITA", rep.id + " sem heartbeat há " + elapsed + "ms");
            }
        }

        // 2. Verificar subreplicação e excesso de cópias
        PriorityQueue<Catalog.Entry> underReplicated = new PriorityQueue<>(
                Comparator.comparingInt(this::countNonDeadCopies)
        );

        for (Catalog.Entry entry : catalog.all()) {
            int aliveCopies = countNonDeadCopies(entry);
            if (aliveCopies < rTarget) {
                eventLogger.accept("SUBREPLICADO", entry.chunkId + " tem " + aliveCopies + "/" + rTarget + " cópias");
                underReplicated.add(entry);
            } else if (aliveCopies > rTarget) {
                int excess = aliveCopies - rTarget;
                eventLogger.accept("EXCESSO", entry.chunkId + " tem " + aliveCopies + "/" + rTarget + " cópias");
                List<ReplicaRegistry.ReplicaInfo> holders = new ArrayList<>();
                for (String repId : entry.replicas) {
                    ReplicaRegistry.ReplicaInfo info = registry.get(repId);
                    if (info != null && info.state != ReplicaRegistry.State.MORTA) {
                        holders.add(info);
                    }
                }
                // Remove das réplicas com menos espaço livre
                holders.sort(Comparator.comparingLong(ReplicaRegistry.ReplicaInfo::freeBytes));
                for (int i = 0; i < Math.min(excess, holders.size()); i++) {
                    String victim = holders.get(i).id;
                    hub.publish(Wire.topicReplica(victim), Wire.encodeDelete(entry.chunkId));
                    catalog.removeReplicaFromChunk(entry.chunkId, victim);
                    eventLogger.accept("REMOVIDO", "chunk=" + entry.chunkId + " replica=" + victim);
                }
            }
        }

        // 3. Reparar até Wire.REPARACOES chunks por ronda
        int repairedCount = 0;
        while (!underReplicated.isEmpty() && repairedCount < Wire.REPARACOES) {
            Catalog.Entry entry = underReplicated.poll();
            int currentCopies = countNonDeadCopies(entry);
            int needed = rTarget - currentCopies;
            if (needed <= 0) continue;

            Set<String> exclude = new HashSet<>(entry.replicas);
            List<String> targets = registry.reserveAndChoose(entry.chunkId, entry.tamanho, needed, exclude, placement);
            if (targets.isEmpty()) continue;

            Path localFile = sealedDir.resolve(entry.chunkId + ".bin");
            if (Files.exists(localFile)) {
                String storeMsg = buildStoreMessageFromSealed(entry.chunkId);
                for (String target : targets) {
                    hub.publish(Wire.topicReplica(target), storeMsg);
                    eventLogger.accept("REPARACAO", "chunk=" + entry.chunkId + " origem=lider destino=" + target);
                    scheduleReservationRelease(entry.chunkId, target, entry.tamanho);
                }
            } else {
                // Pedir FETCH a uma réplica viva que tenha o chunk
                String source = findAliveHolder(entry);
                if (source != null) {
                    for (String target : targets) {
                        String reqId = "rep:" + entry.chunkId + ":" + target + ":" + UUID.randomUUID();
                        fetchRequester.accept(reqId, source);
                        eventLogger.accept("REPARACAO", "chunk=" + entry.chunkId + " origem=" + source + " destino=" + target);
                        scheduleReservationRelease(entry.chunkId, target, entry.tamanho);
                    }
                } else {
                    for (String target : targets) {
                        registry.releaseReservation(entry.chunkId, target, entry.tamanho);
                    }
                }
            }
            repairedCount++;
        }

        // 4. Enviar um desafio POSSE? a uma réplica aleatória
        sendRandomPossessionChallenge();

        // 5. Publicar MANIFESTO se mudou e passou MANIFESTO_MS
        if (now - lastManifestPublishMs >= Wire.MANIFESTO_MS && catalog.consumeDirtyFlag()) {
            publishManifest();
            lastManifestPublishMs = now;
        }
    }

    private ChunkCrypto.Sealed reopenOrWrap(String chunkId, byte[] rawFileBytes) {
        // O ficheiro em selados/<chunkId>.bin guarda: keyVersion;saltB64;nonceB64;ciphertextB64
        try {
            String text = new String(rawFileBytes, StandardCharsets.UTF_8);
            String[] p = text.split(";", 4);
            byte[] salt = Base64.getDecoder().decode(p[1]);
            byte[] nonce = Base64.getDecoder().decode(p[2]);
            byte[] ct = Base64.getDecoder().decode(p[3]);
            return new ChunkCrypto.Sealed(chunkId, Integer.parseInt(p[0]), salt, nonce, ct);
        } catch (Exception e) {
            throw new IllegalStateException("Formato inválido em selados/" + chunkId, e);
        }
    }

    public byte[] readSealedCiphertext(String chunkId) throws Exception {
        Path localFile = sealedDir.resolve(chunkId + ".bin");
        if (!Files.exists(localFile)) return null;
        return reopenOrWrap(chunkId, Files.readAllBytes(localFile)).ciphertext;
    }

    public String buildStoreMessageFromSealed(String chunkId) throws Exception {
        Path localFile = sealedDir.resolve(chunkId + ".bin");
        if (!Files.exists(localFile)) return null;
        String text = Files.readString(localFile, StandardCharsets.UTF_8);
        String[] p = text.split(";", 4);
        byte[] ct = Base64.getDecoder().decode(p[3]);
        return Wire.encodeStore(chunkId, Integer.parseInt(p[0]), p[1], p[2], ct.length, p[3]);
    }

    private void scheduleReservationRelease(String chunkId, String target, long size) {
        scheduler.schedule(() -> registry.releaseReservation(chunkId, target, size),
                Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private int countNonDeadCopies(Catalog.Entry entry) {
        int count = 0;
        for (String repId : entry.replicas) {
            ReplicaRegistry.ReplicaInfo info = registry.get(repId);
            if (info != null && info.state != ReplicaRegistry.State.MORTA) {
                count++;
            }
        }
        return count;
    }

    private String findAliveHolder(Catalog.Entry entry) {
        for (String repId : entry.replicas) {
            ReplicaRegistry.ReplicaInfo info = registry.get(repId);
            if (info != null && info.state == ReplicaRegistry.State.VIVA) {
                return repId;
            }
        }
        return null;
    }

    private void sendRandomPossessionChallenge() {
        List<Catalog.Entry> allEntries = catalog.all();
        if (allEntries.isEmpty()) return;
        Collections.shuffle(allEntries, random);

        for (Catalog.Entry entry : allEntries) {
            List<String> aliveHolders = new ArrayList<>();
            for (String repId : entry.replicas) {
                ReplicaRegistry.ReplicaInfo info = registry.get(repId);
                if (info != null && info.state == ReplicaRegistry.State.VIVA) {
                    aliveHolders.add(repId);
                }
            }
            if (aliveHolders.isEmpty()) continue;

            Catalog.Challenge ch = catalog.nextUnusedChallenge(entry.chunkId);
            if (ch == null) continue;

            String target = aliveHolders.get(random.nextInt(aliveHolders.size()));
            String pedido = UUID.randomUUID().toString();
            pendingPosses.put(pedido, new PendingPosse(target, entry.chunkId, ch.esperadoHex));
            hub.publish(Wire.topicReplica(target),
                    Wire.encodePosseReq(pedido, entry.chunkId, ch.inicio, ch.comprimento, ch.nonceB64));

            scheduler.schedule(() -> {
                PendingPosse expired = pendingPosses.remove(pedido);
                if (expired != null) {
                    eventLogger.accept("POSSE_FALHOU", "timeout replica=" + expired.replicaId + " chunk=" + expired.chunkId);
                    catalog.removeReplicaFromChunk(expired.chunkId, expired.replicaId);
                }
            }, Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            break;
        }
    }

    public void handlePosseResponse(String replicaId, String pedido, String sha256Hex) {
        PendingPosse p = pendingPosses.remove(pedido);
        if (p == null) return;
        if (!p.replicaId.equals(replicaId) || !p.esperadoHex.equalsIgnoreCase(sha256Hex)) {
            eventLogger.accept("POSSE_FALHOU", "hash incorreto replica=" + replicaId + " chunk=" + p.chunkId);
            catalog.removeReplicaFromChunk(p.chunkId, p.replicaId);
        }
    }

    public void handleInventario(String replicaId, List<String> chunkIds) {
        Set<String> reported = new HashSet<>(chunkIds);
        for (String cid : reported) {
            Catalog.Entry entry = catalog.get(cid);
            if (entry != null) {
                catalog.confirmReplica(cid, replicaId, Wire.W);
            } else {
                hub.publish(Wire.topicReplica(replicaId), Wire.encodeDelete(cid));
                eventLogger.accept("REMOVIDO", "chunk desconhecido=" + cid + " replica=" + replicaId);
            }
        }
        // Se o catálogo tinha este chunk nesta réplica mas ela já não o tem, remover
        for (Catalog.Entry entry : catalog.all()) {
            if (entry.replicas.contains(replicaId) && !reported.contains(entry.chunkId)) {
                catalog.removeReplicaFromChunk(entry.chunkId, replicaId);
            }
        }
    }

    public void publishManifest() throws Exception {
        manifestVersion++;
        byte[] plain = catalog.serializeManifest().getBytes(StandardCharsets.UTF_8);
        ChunkCrypto.Sealed sealed = crypto.seal(plain);
        String msg = Wire.encodeManifesto(
                manifestVersion, sealed.id, sealed.keyVersion,
                Base64.getEncoder().encodeToString(sealed.salt),
                Base64.getEncoder().encodeToString(sealed.nonce),
                Base64.getEncoder().encodeToString(sealed.ciphertext)
        );
        for (ReplicaRegistry.ReplicaInfo rep : registry.all()) {
            if (rep.state != ReplicaRegistry.State.MORTA) {
                hub.publish(Wire.topicReplica(rep.id), msg);
            }
        }
        eventLogger.accept("MANIFESTO", "versao=" + manifestVersion + " chunks=" + catalog.all().size());
    }

    public void setManifestVersion(int v) {
        this.manifestVersion = Math.max(this.manifestVersion, v);
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
