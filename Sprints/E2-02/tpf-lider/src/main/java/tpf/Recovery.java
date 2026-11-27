package tpf;

import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public final class Recovery {

    public static final class DataPayload {
        public final String replicaId;
        public final String chunkId;
        public final int versaoChave;
        public final byte[] salt;
        public final byte[] nonce;
        public final byte[] ciphertext;

        public DataPayload(String replicaId, String chunkId, int versaoChave, byte[] salt, byte[] nonce, byte[] ciphertext) {
            this.replicaId = replicaId;
            this.chunkId = chunkId;
            this.versaoChave = versaoChave;
            this.salt = salt;
            this.nonce = nonce;
            this.ciphertext = ciphertext;
        }
    }

    public static final class ManifestPayload {
        public final String replicaId;
        public final int versao;
        public final ChunkCrypto.Sealed sealed;

        public ManifestPayload(String replicaId, int versao, ChunkCrypto.Sealed sealed) {
            this.replicaId = replicaId;
            this.versao = versao;
            this.sealed = sealed;
        }
    }

    private final Path logFile;
    private final ReplicaRegistry registry;
    private final Catalog catalog;
    private final ChunkCrypto crypto;
    private final PubSubHub hub;
    private final BiConsumer<String, String> eventLogger;

    private final Map<String, CompletableFuture<DataPayload>> pendingFetches = new ConcurrentHashMap<>();
    private final Map<String, List<ManifestPayload>> pendingManifests = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<String>>> pendingInventories = new ConcurrentHashMap<>();

    public Recovery(Path logFile, ReplicaRegistry registry, Catalog catalog, ChunkCrypto crypto,
                    PubSubHub hub, BiConsumer<String, String> eventLogger) {
        this.logFile = logFile;
        this.registry = registry;
        this.catalog = catalog;
        this.crypto = crypto;
        this.hub = hub;
        this.eventLogger = eventLogger;
    }

    public CompletableFuture<DataPayload> requestFetch(String pedido, String replicaId, String chunkId) {
        CompletableFuture<DataPayload> fut = new CompletableFuture<>();
        pendingFetches.put(pedido, fut);
        hub.publish(Wire.topicReplica(replicaId), Wire.encodeFetch(pedido, chunkId));
        return fut;
    }

    public void onDataReceived(String replicaId, String pedido, String chunkId, int versaoChave,
                               String saltB64, String nonceB64, String dadosB64) {
        CompletableFuture<DataPayload> fut = pendingFetches.remove(pedido);
        if (fut != null) {
            fut.complete(new DataPayload(
                    replicaId, chunkId, versaoChave,
                    Base64.getDecoder().decode(saltB64),
                    Base64.getDecoder().decode(nonceB64),
                    Base64.getDecoder().decode(dadosB64)
            ));
        }
    }

    public void onManifestoReceived(String replicaId, int versao, String chunkId, int versaoChave,
                                    String saltB64, String nonceB64, String dadosB64) {
        ChunkCrypto.Sealed s = new ChunkCrypto.Sealed(
                chunkId, versaoChave,
                Base64.getDecoder().decode(saltB64),
                Base64.getDecoder().decode(nonceB64),
                Base64.getDecoder().decode(dadosB64)
        );
        for (List<ManifestPayload> list : pendingManifests.values()) {
            list.add(new ManifestPayload(replicaId, versao, s));
        }
    }

    public void onInventarioReceived(String replicaId, String pedido, List<String> chunkIds) {
        Map<String, List<String>> map = pendingInventories.get(pedido);
        if (map != null) {
            map.put(replicaId, chunkIds);
        }
    }

    public List<Chunk.LogEntry> recuperar(long de, long ate) throws Exception {
        List<Chunk.LogEntry> result = new ArrayList<>();
        for (Catalog.Entry entry : catalog.all()) {
            if (entry.lsnAte < de || entry.lsnDe > ate) continue;
            byte[] plain = fetchAndOpenChunk(entry);
            if (plain == null) {
                throw new IllegalStateException("Não foi possível recuperar o chunk " + entry.chunkId);
            }
            for (Chunk.LogEntry le : Chunk.unpack(plain)) {
                if (le.lsn >= de && le.lsn <= ate) {
                    result.add(le);
                }
            }
        }
        result.sort(Comparator.comparingLong(e -> e.lsn));
        return result;
    }

    private byte[] fetchAndOpenChunk(Catalog.Entry entry) {
        List<ReplicaRegistry.ReplicaInfo> candidates = new ArrayList<>();
        for (String repId : entry.replicas) {
            ReplicaRegistry.ReplicaInfo info = registry.get(repId);
            if (info != null && info.state != ReplicaRegistry.State.MORTA) {
                candidates.add(info);
            }
        }
        // Ordenar pela réplica com menor tempo de resposta
        candidates.sort(Comparator.comparingDouble(r -> r.monitor.getEwmaLatencyMs()));

        for (ReplicaRegistry.ReplicaInfo rep : candidates) {
            String pedido = UUID.randomUUID().toString();
            try {
                DataPayload dp = requestFetch(pedido, rep.id, entry.chunkId)
                        .get(Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                ChunkCrypto.Sealed s = new ChunkCrypto.Sealed(
                        dp.chunkId, dp.versaoChave, dp.salt, dp.nonce, dp.ciphertext);
                return crypto.open(s);
            } catch (Exception e) {
                pendingFetches.remove(pedido);
                eventLogger.accept("INTEGRIDADE", "Falha ao obter/decifrar chunk=" + entry.chunkId + " replica=" + rep.id + " (" + e.getMessage() + ")");
            }
        }
        return null;
    }

    public long recuperarTudo(Reconciler reconciler) throws Exception {
        System.out.println("Iniciando recuperação total a partir das réplicas...");
        // Aguarda brevemente para as réplicas estabelecerem ligação WebSocket
        Thread.sleep(2500L);

        String pedidoMan = UUID.randomUUID().toString();
        List<ManifestPayload> respostas = Collections.synchronizedList(new ArrayList<>());
        pendingManifests.put(pedidoMan, respostas);
        hub.publish(Wire.TOPICO_CTL, Wire.encodeManifestoReq(pedidoMan));

        Thread.sleep(Wire.ESPERA_MANIFESTO_MS);
        pendingManifests.remove(pedidoMan);

        ManifestPayload best = null;
        String bestPlain = null;
        for (ManifestPayload mp : respostas) {
            try {
                byte[] plainBytes = crypto.open(mp.sealed);
                if (best == null || mp.versao > best.versao) {
                    best = mp;
                    bestPlain = new String(plainBytes, StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                eventLogger.accept("INTEGRIDADE", "Manifesto inválido de " + mp.replicaId + ": " + e.getMessage());
            }
        }

        if (best == null) {
            eventLogger.accept("RECUPERADO", "Nenhum manifesto válido encontrado; arranque vazio.");
            return 0L;
        }

        catalog.restoreFromManifest(bestPlain);
        reconciler.setManifestVersion(best.versao);

        // Pedir INVENTARIO? a todas para reconciliar o que mudou desde esse manifesto
        String pedidoInv = UUID.randomUUID().toString();
        Map<String, List<String>> invMap = new ConcurrentHashMap<>();
        pendingInventories.put(pedidoInv, invMap);
        hub.publish(Wire.TOPICO_CTL, Wire.encodeInventarioReq(pedidoInv));
        Thread.sleep(Math.min(3000L, Wire.ESPERA_MANIFESTO_MS));
        pendingInventories.remove(pedidoInv);

        for (Map.Entry<String, List<String>> e : invMap.entrySet()) {
            reconciler.handleInventario(e.getKey(), e.getValue());
        }

        // Obter todos os chunks e reconstruir registos.log
        List<Chunk.LogEntry> allEntries = new ArrayList<>();
        for (Catalog.Entry entry : catalog.all()) {
            byte[] plain = fetchAndOpenChunk(entry);
            if (plain != null) {
                allEntries.addAll(Chunk.unpack(plain));
            }
        }
        allEntries.sort(Comparator.comparingLong(e -> e.lsn));

        List<String> lines = new ArrayList<>();
        long maxLsn = 0L;
        long minLsn = allEntries.isEmpty() ? 0L : allEntries.get(0).lsn;
        for (Chunk.LogEntry le : allEntries) {
            lines.add(le.toLogLine());
            maxLsn = Math.max(maxLsn, le.lsn);
        }
        Files.write(logFile, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try (FileChannel fc = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
            fc.force(true);
        }

        eventLogger.accept("RECUPERADO", "lsn=" + minLsn + ".." + maxLsn + " registos=" + allEntries.size());
        System.out.println("Recuperação concluída: " + allEntries.size() + " registos restaurados (LSN " + minLsn + ".." + maxLsn + ").");
        return maxLsn;
    }
}
