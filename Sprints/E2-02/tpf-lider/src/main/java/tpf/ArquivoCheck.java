package tpf;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class ArquivoCheck {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: ArquivoCheck <pastaLider> <pastaReplica>... [falhas.txt]");
            System.exit(1);
        }

        Path liderDir = Path.of(args[0]);
        List<Path> replicaDirs = new ArrayList<>();
        Path falhasFile = null;

        for (int i = 1; i < args.length; i++) {
            Path p = Path.of(args[i]);
            if (Files.isRegularFile(p) && p.getFileName().toString().endsWith(".txt")) {
                falhasFile = p;
            } else {
                replicaDirs.add(p);
            }
        }

        System.out.println("====================================================");
        System.out.println("RELATÓRIO ARQUIVOCHECK");
        System.out.println("====================================================");

        // 1. Durabilidade
        Path logPath = liderDir.resolve("registos.log");
        Path catPath = liderDir.resolve("catalogo.txt");
        List<Chunk.LogEntry> logEntries = new ArrayList<>();
        if (Files.exists(logPath)) {
            for (String l : Files.readAllLines(logPath)) {
                if (!l.isBlank()) logEntries.add(Chunk.LogEntry.fromLogLine(l));
            }
        }
        List<Catalog.Entry> catEntries = new ArrayList<>();
        if (Files.exists(catPath)) {
            for (String l : Files.readAllLines(catPath)) {
                if (!l.isBlank()) catEntries.add(Catalog.Entry.fromLine(l));
            }
        }
        System.out.println("\n1. DURABILIDADE:");
        System.out.println("   Total de registos no log final: " + logEntries.size());
        long maxDurableLsn = catEntries.stream()
                .filter(c -> "DURAVEL".equals(c.estado))
                .mapToLong(c -> c.lsnAte).max().orElse(0L);
        long unsealedCount = logEntries.stream().filter(e -> e.lsn > maxDurableLsn).count();
        System.out.println("   Último LSN durável: " + maxDurableLsn + " | Registos ainda não duráveis: " + unsealedCount);

        // 2. Replicação final
        Map<String, Set<String>> diskChunks = new HashMap<>();
        for (Path rDir : replicaDirs) {
            String dirName = rDir.getFileName().toString();
            String repId = dirName.startsWith("dados-") ? dirName.substring(6) : dirName;
            if (Files.exists(rDir)) {
                try (var s = Files.list(rDir)) {
                    for (Path f : s.toList()) {
                        String fn = f.getFileName().toString();
                        if (fn.endsWith(".bin") && !fn.equals("manifesto.bin")) {
                            String cid = fn.substring(0, fn.length() - 4);
                            diskChunks.computeIfAbsent(cid, k -> new TreeSet<>()).add(repId);
                        }
                    }
                }
            }
        }

        int rTarget = Wire.R;
        int lessR = 0, exactR = 0, moreR = 0;
        for (Catalog.Entry ce : catEntries) {
            int n = diskChunks.getOrDefault(ce.chunkId, Set.of()).size();
            if (n < rTarget) lessR++;
            else if (n == rTarget) exactR++;
            else moreR++;
        }
        System.out.println("\n2. REPLICAÇÃO FINAL (R = " + rTarget + "):");
        System.out.println("   Chunks no catálogo: " + catEntries.size() + " | <R: " + lessR + " | ==R: " + exactR + " | >R: " + moreR);

        // 3. Ocupação e desvio-padrão
        Map<String, Long> quotas = new HashMap<>();
        Path repTxt = liderDir.resolve("replicas.txt");
        if (Files.exists(repTxt)) {
            for (String l : Files.readAllLines(repTxt)) {
                if (!l.isBlank()) {
                    String[] p = l.split(";", 3);
                    quotas.put(p[0], Long.parseLong(p[2]));
                }
            }
        }
        System.out.println("\n3. OCUPAÇÃO POR RÉPLICA:");
        List<Double> pcts = new ArrayList<>();
        for (Path rDir : replicaDirs) {
            String dirName = rDir.getFileName().toString();
            String repId = dirName.startsWith("dados-") ? dirName.substring(6) : dirName;
            long used = 0L;
            if (Files.exists(rDir)) {
                try (var s = Files.list(rDir)) {
                    for (Path f : s.toList()) {
                        String fn = f.getFileName().toString();
                        if (fn.endsWith(".bin") && !fn.equals("manifesto.bin")) used += Files.size(f);
                    }
                }
            }
            long q = quotas.getOrDefault(repId, 0L);
            double pct = q > 0 ? (100.0 * used / q) : 0.0;
            pcts.add(pct);
            System.out.printf("   Réplica %s: %d / %d bytes (%.2f%%)%n", repId, used, q, pct);
        }
        double mean = pcts.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = pcts.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0.0);
        System.out.printf("   Desvio-padrão das ocupações: %.2f pontos percentuais%n", Math.sqrt(variance));

        // 4. Reparações
        System.out.println("\n4. REPARAÇÕES:");
        Path evLider = liderDir.resolve("eventos-lider.txt");
        List<String> liderEvents = Files.exists(evLider) ? Files.readAllLines(evLider) : List.of();
        long repCount = liderEvents.stream().filter(l -> l.contains(";REPARACAO;")).count();
        System.out.println("   Total de operações REPARACAO registadas: " + repCount);
        if (falhasFile != null && Files.exists(falhasFile)) {
            for (String fl : Files.readAllLines(falhasFile)) {
                if (!fl.isBlank()) System.out.println("   Falha injetada: " + fl);
            }
        }

        // 5. Verificação Zero-Knowledge
        System.out.println("\n5. ZERO-KNOWLEDGE:");
        Set<String> sensitiveTokens = new HashSet<>();
        for (Chunk.LogEntry le : logEntries) {
            String plain = le.decodedText();
            if (plain.length() > 8) sensitiveTokens.add(plain);
        }
        int leaks = 0;
        for (Path rDir : replicaDirs) {
            if (!Files.exists(rDir)) continue;
            try (var s = Files.list(rDir)) {
                for (Path f : s.toList()) {
                    if (Files.isRegularFile(f)) {
                        String content = new String(Files.readAllBytes(f), StandardCharsets.ISO_8859_1);
                        for (String tok : sensitiveTokens) {
                            if (content.contains(tok)) leaks++;
                        }
                    }
                }
            }
        }
        System.out.println("   Ocorrências de texto em claro (>8 chars) nas réplicas: " + leaks);

        // 6. Integridade
        System.out.println("\n6. INTEGRIDADE E POSSE:");
        long integCount = liderEvents.stream().filter(l -> l.contains(";INTEGRIDADE;")).count();
        long posseFailCount = liderEvents.stream().filter(l -> l.contains(";POSSE_FALHOU;")).count();
        System.out.println("   Eventos INTEGRIDADE: " + integCount + " | Eventos POSSE_FALHOU: " + posseFailCount);
        System.out.println("====================================================");
    }
}
