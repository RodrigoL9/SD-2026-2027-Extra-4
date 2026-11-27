package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

public final class ReplicaRegistry {

    public enum State {
        VIVA, SUSPEITA, MORTA
    }

    public static final class ReplicaInfo {
        public final String id;
        public final String publicKeyB64;
        public final PublicKey publicKey;
        public final HeartbeatMonitor monitor;
        public volatile State state;
        public volatile long quota;
        public volatile long usedBytes;
        public volatile long reservedBytes;
        public volatile int nChunks;
        public volatile int manifestVersion;

        public ReplicaInfo(String id, String publicKeyB64, PublicKey publicKey, long quota, long nowMs) {
            this.id = id;
            this.publicKeyB64 = publicKeyB64;
            this.publicKey = publicKey;
            this.quota = quota;
            this.state = State.VIVA;
            this.monitor = new HeartbeatMonitor(nowMs);
        }

        public long freeBytes() {
            return Math.max(0L, quota - usedBytes - reservedBytes);
        }
    }

    private static final long TOKEN_TTL_MS = 10 * 60 * 1000L;
    private static final SecureRandom RNG = new SecureRandom();

    private final Path replicasFile;
    private final Map<String, ReplicaInfo> replicas = new LinkedHashMap<>();
    private final Map<String, Long> pendingTokens = new HashMap<>();
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
            long quota = p.length > 2 ? Long.parseLong(p[2]) : 0L;
            PublicKey pk = decodePublicKey(pubB64);
            replicas.put(id, new ReplicaInfo(id, pubB64, pk, quota, System.currentTimeMillis()));
        }
    }

    public synchronized void saveToDisk() {
        try {
            List<String> lines = new ArrayList<>();
            for (ReplicaInfo r : replicas.values()) {
                lines.add(r.id + ";" + r.publicKeyB64 + ";" + r.quota);
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

    public synchronized String generateToken() {
        byte[] raw = new byte[18];
        RNG.nextBytes(raw);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        pendingTokens.put(token, System.currentTimeMillis());
        return token;
    }

    public synchronized PublicKey authenticateOrRegister(String id, String pubB64, String token) throws Exception {
        long now = System.currentTimeMillis();
        pendingTokens.entrySet().removeIf(e -> now - e.getValue() > TOKEN_TTL_MS);

        if (token != null && !token.isBlank()) {
            Long created = pendingTokens.remove(token);
            if (created == null || now - created > TOKEN_TTL_MS) {
                throw new SecurityException("Token de registo inválido, expirado ou já utilizado");
            }
            PublicKey pk = decodePublicKey(pubB64);
            ReplicaInfo info = new ReplicaInfo(id, pubB64, pk, 0L, now);
            replicas.put(id, info);
            saveToDisk();
            return pk;
        }

        ReplicaInfo existing = replicas.get(id);
        if (existing == null) {
            throw new SecurityException("Réplica não registada: " + id);
        }
        if (!existing.publicKeyB64.equals(pubB64)) {
            throw new SecurityException("Chave pública não coincide para réplica: " + id);
        }
        return existing.publicKey;
    }

    public synchronized State updateHeartbeat(String id, long sentMs, long quota, long used, int nChunks, int manifestVer) {
        ReplicaInfo r = replicas.get(id);
        if (r == null) return null;
        long now = System.currentTimeMillis();
        State prev = r.state;
        r.monitor.recordHeartbeat(sentMs, now);
        boolean quotaChanged = r.quota != quota;
        r.quota = quota;
        r.usedBytes = used;
        r.nChunks = nChunks;
        r.manifestVersion = manifestVer;
        r.state = State.VIVA;
        if (quotaChanged) {
            saveToDisk();
        }
        return prev;
    }

    public synchronized List<String> reserveAndChoose(String chunkId, long size, int count,
                                                      Set<String> exclude, Placement placement) {
        List<Placement.Candidate> candidates = new ArrayList<>();
        for (ReplicaInfo r : replicas.values()) {
            if (r.state != State.MORTA && r.quota > 0) {
                candidates.add(new Placement.Candidate(r.id, r.freeBytes()));
            }
        }
        List<String> chosen = placement.choose(chunkId, size, count, candidates, exclude);
        for (String id : chosen) {
            ReplicaInfo r = replicas.get(id);
            if (r != null) {
                String key = chunkId + ":" + id;
                if (activeReservations.add(key)) {
                    r.reservedBytes += size;
                }
            }
        }
        return chosen;
    }

    public synchronized void releaseReservation(String chunkId, String replicaId, long size) {
        String key = chunkId + ":" + replicaId;
        if (activeReservations.remove(key)) {
            ReplicaInfo r = replicas.get(replicaId);
            if (r != null) {
                r.reservedBytes = Math.max(0L, r.reservedBytes - size);
            }
        }
    }

    public synchronized boolean removeReplica(String id) {
        ReplicaInfo removed = replicas.remove(id);
        if (removed != null) {
            saveToDisk();
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
