package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;

public final class Catalog {

    public static final class Entry {
        public final String chunkId;
        public final long lsnDe;
        public final long lsnAte;
        public final long tamanho;
        public final Set<String> replicas = new LinkedHashSet<>();
        public String estado; // SELADO ou DURAVEL

        public Entry(String chunkId, long lsnDe, long lsnAte, long tamanho, Collection<String> reps, String estado) {
            this.chunkId = chunkId;
            this.lsnDe = lsnDe;
            this.lsnAte = lsnAte;
            this.tamanho = tamanho;
            this.replicas.addAll(reps);
            this.estado = estado;
        }

        public String toLine() {
            return chunkId + ";" + lsnDe + ";" + lsnAte + ";" + tamanho + ";" + String.join(",", replicas) + ";" + estado;
        }

        public static Entry fromLine(String line) {
            String[] p = line.split(";", 6);
            List<String> reps = p[4].isBlank() ? List.of() : Arrays.asList(p[4].split(","));
            return new Entry(p[0], Long.parseLong(p[1]), Long.parseLong(p[2]), Long.parseLong(p[3]), reps, p[5]);
        }
    }

    public static final class Challenge {
        public final String chunkId;
        public final int inicio;
        public final int comprimento;
        public final String nonceB64;
        public final String esperadoHex;
        public boolean usado = false;

        public Challenge(String chunkId, int inicio, int comprimento, String nonceB64, String esperadoHex) {
            this.chunkId = chunkId;
            this.inicio = inicio;
            this.comprimento = comprimento;
            this.nonceB64 = nonceB64;
            this.esperadoHex = esperadoHex;
        }

        public String toLine() {
            return chunkId + ";" + inicio + ";" + comprimento + ";" + nonceB64 + ";" + esperadoHex;
        }

        public static Challenge fromLine(String line) {
            String[] p = line.split(";", 5);
            return new Challenge(p[0], Integer.parseInt(p[1]), Integer.parseInt(p[2]), p[3], p[4]);
        }
    }

    private static final SecureRandom RNG = new SecureRandom();

    private final Path catalogFile;
    private final Path challengesFile;
    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private final Map<String, List<Challenge>> challenges = new HashMap<>();
    private boolean dirty = false;

    public Catalog(Path baseDir) throws IOException {
        this.catalogFile = baseDir.resolve("catalogo.txt");
        this.challengesFile = baseDir.resolve("desafios.txt");
        loadFromDisk();
    }

    public boolean existsOnDisk() {
        return Files.exists(catalogFile);
    }

    private synchronized void loadFromDisk() throws IOException {
        if (Files.exists(catalogFile)) {
            for (String line : Files.readAllLines(catalogFile)) {
                if (!line.isBlank()) {
                    Entry e = Entry.fromLine(line);
                    entries.put(e.chunkId, e);
                }
            }
        }
        if (Files.exists(challengesFile)) {
            for (String line : Files.readAllLines(challengesFile)) {
                if (!line.isBlank()) {
                    Challenge c = Challenge.fromLine(line);
                    challenges.computeIfAbsent(c.chunkId, k -> new ArrayList<>()).add(c);
                }
            }
        }
    }

    public synchronized void saveAtomically() {
        try {
            List<String> lines = new ArrayList<>();
            for (Entry e : entries.values()) {
                lines.add(e.toLine());
            }
            Path tmp = catalogFile.resolveSibling("catalogo.txt.tmp");
            Files.write(tmp, lines);
            Files.move(tmp, catalogFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Erro ao gravar catalogo.txt: " + e.getMessage());
        }
    }

    public synchronized void addSealedChunk(String chunkId, long lsnDe, long lsnAte, byte[] ciphertext) throws Exception {
        Entry e = new Entry(chunkId, lsnDe, lsnAte, ciphertext.length, List.of(), "SELADO");
        entries.put(chunkId, e);
        dirty = true;
        saveAtomically();

        // Pré-calcula 8 desafios de posse sobre intervalos aleatórios de 1 KB
        List<Challenge> list = new ArrayList<>();
        List<String> lines = new ArrayList<>();
        int len = Math.min(1024, ciphertext.length);
        int maxStart = Math.max(0, ciphertext.length - len);
        for (int i = 0; i < 8; i++) {
            int inicio = maxStart == 0 ? 0 : RNG.nextInt(maxStart + 1);
            byte[] nonce = new byte[16];
            RNG.nextBytes(nonce);
            String nonceB64 = Base64.getEncoder().encodeToString(nonce);

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(nonce);
            md.update(ciphertext, inicio, len);
            String esperado = HexFormat.of().formatHex(md.digest());

            Challenge ch = new Challenge(chunkId, inicio, len, nonceB64, esperado);
            list.add(ch);
            lines.add(ch.toLine());
        }
        challenges.put(chunkId, list);
        Files.write(challengesFile, lines, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized boolean confirmReplica(String chunkId, String replicaId, int wThreshold) {
        Entry e = entries.get(chunkId);
        if (e == null) return false;
        boolean added = e.replicas.add(replicaId);
        boolean becameDurable = false;
        if (e.replicas.size() >= wThreshold && !"DURAVEL".equals(e.estado)) {
            e.estado = "DURAVEL";
            becameDurable = true;
        }
        if (added || becameDurable) {
            dirty = true;
            saveAtomically();
        }
        return becameDurable;
    }

    public synchronized void removeReplicaFromChunk(String chunkId, String replicaId) {
        Entry e = entries.get(chunkId);
        if (e != null && e.replicas.remove(replicaId)) {
            dirty = true;
            saveAtomically();
        }
    }

    public synchronized void removeReplicaEverywhere(String replicaId) {
        boolean changed = false;
        for (Entry e : entries.values()) {
            if (e.replicas.remove(replicaId)) {
                changed = true;
            }
        }
        if (changed) {
            dirty = true;
            saveAtomically();
        }
    }

    public synchronized Challenge nextUnusedChallenge(String chunkId) {
        List<Challenge> list = challenges.get(chunkId);
        if (list == null) return null;
        for (Challenge c : list) {
            if (!c.usado) {
                c.usado = true;
                return c;
            }
        }
        return null;
    }

    public synchronized Entry get(String chunkId) {
        return entries.get(chunkId);
    }

    public synchronized List<Entry> all() {
        return new ArrayList<>(entries.values());
    }

    public synchronized String serializeManifest() {
        StringBuilder sb = new StringBuilder();
        for (Entry e : entries.values()) {
            sb.append(e.toLine()).append('\n');
        }
        return sb.toString();
    }

    public synchronized void restoreFromManifest(String manifestText) {
        entries.clear();
        for (String line : manifestText.split("\n")) {
            if (!line.isBlank()) {
                Entry e = Entry.fromLine(line);
                entries.put(e.chunkId, e);
            }
        }
        dirty = true;
        saveAtomically();
    }

    public synchronized boolean consumeDirtyFlag() {
        boolean was = dirty;
        dirty = false;
        return was;
    }

    public synchronized void markDirty() {
        dirty = true;
    }
}
