package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

public final class Catalog {

    public static final class EntradaCatalogo {
        public final String chunkId;
        public final long lsnDe;
        public final long lsnAte;
        public final long tamanho;
        public final Set<String> replicas = new LinkedHashSet<>();
        public String estado; // SELADO ou DURAVEL

        public EntradaCatalogo(String chunkId, long lsnDe, long lsnAte, long tamanho, Collection<String> reps, String estado) {
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

        public static EntradaCatalogo fromLine(String line) {
            String[] p = line.split(";", 6);
            List<String> reps = p[4].isBlank() ? List.of() : Arrays.asList(p[4].split(","));
            return new EntradaCatalogo(p[0], Long.parseLong(p[1]), Long.parseLong(p[2]), Long.parseLong(p[3]), reps, p[5]);
        }
    }

    private final Path catalogFile;
    private final Map<String, EntradaCatalogo> entries = new LinkedHashMap<>();

    public Catalog(Path baseDir) throws IOException {
        this.catalogFile = baseDir.resolve("catalogo.txt");
        loadFromDisk();
    }

    private synchronized void loadFromDisk() throws IOException {
        if (!Files.exists(catalogFile)) return;
        for (String line : Files.readAllLines(catalogFile)) {
            if (!line.isBlank()) {
                EntradaCatalogo e = EntradaCatalogo.fromLine(line);
                entries.put(e.chunkId, e);
            }
        }
    }

    public synchronized void gravarAtomico() {
        try {
            List<String> lines = new ArrayList<>();
            for (EntradaCatalogo e : entries.values()) {
                lines.add(e.toLine());
            }
            Path tmp = catalogFile.resolveSibling("catalogo.txt.tmp");
            Files.write(tmp, lines);
            Files.move(tmp, catalogFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Erro ao gravar catalogo.txt: " + e.getMessage());
        }
    }

    public synchronized void adicionar(String chunkId, long lsnDe, long lsnAte, long tamanho) {
        EntradaCatalogo e = new EntradaCatalogo(chunkId, lsnDe, lsnAte, tamanho, List.of(), "SELADO");
        entries.put(chunkId, e);
        gravarAtomico();
    }

    public synchronized boolean confirmar(String chunkId, String replicaId, int quorumDuravel) {
        EntradaCatalogo e = entries.get(chunkId);
        if (e == null) return false;
        boolean added = e.replicas.add(replicaId);
        boolean becameDurable = false;
        if (e.replicas.size() >= quorumDuravel && !"DURAVEL".equals(e.estado)) {
            e.estado = "DURAVEL";
            becameDurable = true;
        }
        if (added || becameDurable) {
            gravarAtomico();
        }
        return becameDurable;
    }

    public synchronized Set<String> copias(String chunkId) {
        EntradaCatalogo e = entries.get(chunkId);
        return e == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(e.replicas));
    }

    public synchronized EntradaCatalogo get(String chunkId) {
        return entries.get(chunkId);
    }

    public synchronized List<EntradaCatalogo> all() {
        return new ArrayList<>(entries.values());
    }
}