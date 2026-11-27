package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.function.BiConsumer;

public final class ReplicaRegistry {

    public enum State {
        VIVA, SUSPEITA, MORTA
    }

    public static final class ReplicaInfo {
        public final String id;
        public final String publicKeyB64;
        public final PublicKey publicKey;
        public final HeartbeatMonitor monitor;
        public volatile long quota;
        public volatile long usedBytes;
        public volatile long reservedBytes;

        public ReplicaInfo(String id, String publicKeyB64, PublicKey publicKey, long nowMs) {
            this.id = id;
            this.publicKeyB64 = publicKeyB64;
            this.publicKey = publicKey;
            this.monitor = new HeartbeatMonitor(nowMs);
        }

        public long freeBytes() {
            return Math.max(0L, quota - usedBytes - reservedBytes);
        }
    }

    private final Path replicasFile;
    private final Map<String, ReplicaInfo> replicas = new LinkedHashMap<>();
    private final Set<String> activeReservations = new HashSet<>();

    public ReplicaRegistry(Path baseDir) throws Exception {
        this.replicasFile = baseDir.resolve("replicas.txt");
        loadFromDisk();
    }

    private synchronized void loadFromDisk() throws Exception {
        if (!Files.exists(replicasFile)) return;
        for (String line : Files.readAllLines(replicasFile)) {
            if (line.isBlank()) continue;
            String[] p = line.split(";", 3);
            String id = p[0];
            String pubB64 = p[1];
            PublicKey pk = decodePublicKey(pubB64);
            replicas.put(id, new ReplicaInfo(id, pubB64, pk, System.currentTimeMillis()));
        }
    }

    public synchronized void gravarAtomico() {
        try {
            List<String> lines = new ArrayList<>();
            for (ReplicaInfo r : replicas.values()) {
                lines.add(r.id + ";" + r.publicKeyB64);
            }
            Path tmp = replicasFile.resolveSibling("replicas.txt.tmp");
            Files.write(tmp, lines);
            Files.move(tmp, replicasFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Erro ao gravar replicas.txt: " + e.getMessage());
        }
    }

    public static PublicKey decodePublicKey(String pubB64) throws Exception {
        byte[] der = Base64.getDecoder().decode(pubB64);
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der));
    }

    public synchronized boolean isRegistered(String id) {
        return replicas.containsKey(id);
    }

    public synchronized PublicKey getPublicKey(String id) {
        ReplicaInfo r = replicas.get(id);
        return r != null ? r.publicKey : null;
    }

    public synchronized void registerReplica(String id, String pubB64, PublicKey pk) {
        if (replicas.containsKey(id)) {
            throw new SecurityException("Réplica já registada: " + id);
        }
        replicas.put(id, new ReplicaInfo(id, pubB64, pk, System.currentTimeMillis()));
        gravarAtomico();
    }

    public synchronized void onHeartbeat(String id, long usados, long quota, BiConsumer<String, String> eventLogger) {
        ReplicaInfo r = replicas.get(id);
        if (r == null) return;
        long now = System.currentTimeMillis();
        State antigo = r.monitor.onHeartbeat(now);
        r.quota = quota;
        r.usedBytes = usados;
        if (antigo != null && eventLogger != null) {
            eventLogger.accept("ESTADO", id + " " + antigo + " -> VIVA");
        }
    }

    public synchronized void atualizarEstados(long agora, BiConsumer<String, String> eventLogger) {
        for (ReplicaInfo r : replicas.values()) {
            State antigo = r.monitor.atualizarEstado(agora);
            if (antigo != null && eventLogger != null) {
                eventLogger.accept("ESTADO", r.id + " " + antigo + " -> " + r.monitor.getState());
            }
        }
    }

    public synchronized List<String> reserveAndChoose(String chunkId, long tamanho, int count,
                                                      Set<String> exclude, Placement placement) {
        List<Placement.Candidate> candidates = new ArrayList<>();
        for (ReplicaInfo r : replicas.values()) {
            if (r.monitor.getState() != State.MORTA && r.quota > 0) {
                candidates.add(new Placement.Candidate(r.id, r.freeBytes()));
            }
        }
        List<String> chosen = placement.choose(chunkId, tamanho, count, candidates, exclude);
        for (String id : chosen) {
            ReplicaInfo r = replicas.get(id);
            if (r != null) {
                String key = chunkId + ":" + id;
                if (activeReservations.add(key)) {
                    r.reservedBytes += tamanho;
                }
            }
        }
        return chosen;
    }

    public synchronized void confirmarReserva(String chunkId, String replicaId, long tamanho) {
        ReplicaInfo r = replicas.get(replicaId);
        if (r == null) return;
        String key = chunkId + ":" + replicaId;
        if (activeReservations.remove(key)) {
            r.reservedBytes = Math.max(0L, r.reservedBytes - tamanho);
        }
        r.usedBytes += tamanho;
    }

    public synchronized void libertar(String chunkId, String replicaId, long tamanho) {
        String key = chunkId + ":" + replicaId;
        if (activeReservations.remove(key)) {
            ReplicaInfo r = replicas.get(replicaId);
            if (r != null) {
                r.reservedBytes = Math.max(0L, r.reservedBytes - tamanho);
            }
        }
    }

    public synchronized boolean removeReplica(String id) {
        ReplicaInfo removed = replicas.remove(id);
        if (removed != null) {
            gravarAtomico();
            return true;
        }
        return false;
    }

    public synchronized ReplicaInfo get(String id) {
        return replicas.get(id);
    }

    public synchronized List<ReplicaInfo> all() {
        return new ArrayList<>(replicas.values());
    }
}