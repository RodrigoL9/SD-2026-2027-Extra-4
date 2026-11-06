package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class RaftCheck {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Uso: java raft04.RaftCheck <N> <pasta> [falhas.txt]");
            System.exit(1);
        }

        int N = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);
        Path falhasFile = (args.length >= 3) ? dir.resolve(args[2]) : null;

        System.out.println("=================================================");
        System.out.println("          RELATÓRIO DE AUDITORIA RAFT            ");
        System.out.println("=================================================");

        // 1. Processar ficheiros de eventos
        Map<Integer, List<String[]>> eventsByPart = new TreeMap<>();
        long earliestStart = Long.MAX_VALUE;

        for (int k = 0; k < N; k++) {
            eventsByPart.put(k, new ArrayList<>());
            for (int i = 1; i <= Wire.GRUPO; i++) {
                Path ev = dir.resolve("eventos-p" + k + "-n" + i + ".txt");
                if (Files.exists(ev)) {
                    List<String> lines = Files.readAllLines(ev, StandardCharsets.UTF_8);
                    for (String l : lines) {
                        if (l.isBlank()) continue;
                        String[] p = l.split(";");
                        if (p.length >= 3) {
                            eventsByPart.get(k).add(p);
                            long ts = Long.parseLong(p[0]);
                            if (ts < earliestStart) earliestStart = ts;
                        }
                    }
                }
            }
            eventsByPart.get(k).sort(Comparator.comparingLong(a -> Long.parseLong(a[0])));
        }

        // 2. Análise por Partição: Sequência de termos, eleições e segurança
        int totalMultiLeaders = 0;
        boolean termNeverDecreases = true;

        for (int k = 0; k < N; k++) {
            System.out.println("\n--- Partição p" + k + " ---");
            List<String[]> evs = eventsByPart.get(k);

            Map<Long, Set<Integer>> candidatesByTerm = new TreeMap<>();
            Map<Long, Set<Integer>> leadersByTerm = new TreeMap<>();
            Map<Long, Long> electionStart = new TreeMap<>();
            Map<Long, Long> leaderTime = new TreeMap<>();
            Long firstLeaderTime = null;

            Map<Integer, Long> nodeLastTerm = new HashMap<>();

            for (String[] e : evs) {
                long ts = Long.parseLong(e[0]);
                String tipo = e[1];

                if ("CANDIDATO".equals(tipo)) {
                    long t = extractKeyLong(e, "termo");
                    candidatesByTerm.computeIfAbsent(t, x -> new HashSet<>());
                    electionStart.putIfAbsent(t, ts);
                } else if ("LIDER".equals(tipo)) {
                    long t = extractKeyLong(e, "termo");
                    int node = extractNodeFromDetail(e);
                    leadersByTerm.computeIfAbsent(t, x -> new HashSet<>()).add(node);
                    leaderTime.putIfAbsent(t, ts);
                    if (firstLeaderTime == null) firstLeaderTime = ts;
                } else if ("TERMO".equals(tipo)) {
                    int node = extractNodeFromFilename(e);
                    String[] diff = e[e.length - 1].split("->");
                    if (diff.length == 2) {
                        long oldT = Long.parseLong(diff[0].trim());
                        long newT = Long.parseLong(diff[1].trim());
                        if (newT < oldT) termNeverDecreases = false;
                        nodeLastTerm.put(node, newT);
                    }
                }
            }

            System.out.println("Sequência de Termos e Eleições:");
            for (Map.Entry<Long, Set<Integer>> entry : candidatesByTerm.entrySet()) {
                long t = entry.getKey();
                Set<Integer> cands = entry.getValue();
                Set<Integer> leds = leadersByTerm.getOrDefault(t, Collections.emptySet());

                if (leds.size() > 1) totalMultiLeaders++;

                String lStr = leds.isEmpty() ? "[voto dividido / sem líder]" : "Líder: n" + leds.iterator().next();
                String durStr = "";
                if (electionStart.containsKey(t) && leaderTime.containsKey(t)) {
                    durStr = " (duração: " + (leaderTime.get(t) - electionStart.get(t)) + " ms)";
                }
                System.out.println("  Termo " + t + ": Candidatos " + cands + " -> " + lStr + durStr);
            }

            if (firstLeaderTime != null && earliestStart != Long.MAX_VALUE) {
                System.out.printf("Tempo até ao 1º líder: %.2f s%n", (firstLeaderTime - earliestStart) / 1000.0);
            } else {
                System.out.println("Tempo até ao 1º líder: Não registado");
            }
        }

        System.out.println("\n--- Segurança Global da Eleição ---");
        System.out.println("Termos com mais de um líder: " + totalMultiLeaders + (totalMultiLeaders == 0 ? " (CORRETO)" : " (FALHA!)"));
        System.out.println("Monotonia de termos mantida: " + (termNeverDecreases ? "SIM (CORRETO)" : "NÃO (FALHA!)"));

        // 3. Leituras Perdidas (produtor.txt vs ficheiros no-*)
        Path prodFile = dir.resolve("produtor.txt");
        if (Files.exists(prodFile)) {
            System.out.println("\n--- Auditoria de Leituras Perdidas ---");
            List<String> prodLines = Files.readAllLines(prodFile, StandardCharsets.UTF_8);

            for (int k = 0; k < N; k++) {
                Set<String> accepted = new HashSet<>();
                for (int i = 1; i <= Wire.GRUPO; i++) {
                    Path noPath = dir.resolve("no-p" + k + "-n" + i + ".txt");
                    if (Files.exists(noPath)) {
                        for (String l : Files.readAllLines(noPath, StandardCharsets.UTF_8)) {
                            if (!l.isBlank()) {
                                String[] f = l.split(";");
                                if (f.length >= 4) {
                                    accepted.add(f[2] + "@" + f[3]); // sensor@temperatura
                                }
                            }
                        }
                    }
                }

                int perdidas = 0;
                int totalProd = 0;
                for (String pl : prodLines) {
                    if (pl.isBlank()) continue;
                    String[] pf = pl.split(";");
                    if (pf.length >= 4 && ("p" + k).equals(pf[1].trim())) {
                        totalProd++;
                        String key = pf[2] + "@" + pf[3];
                        if (!accepted.contains(key)) {
                            perdidas++;
                        }
                    }
                }
                System.out.println("Partição p" + k + ": " + perdidas + " perdidas em " + totalProd + " enviadas.");
            }
        }

        // 4. Coincidência Final de Registos
        System.out.println("\n--- Coincidência Final entre Nós ---");
        for (int k = 0; k < N; k++) {
            List<List<SensorRecord>> nodeRecs = new ArrayList<>();
            long minLastSeq = Long.MAX_VALUE;

            for (int i = 1; i <= Wire.GRUPO; i++) {
                Path p = dir.resolve("no-p" + k + "-n" + i + ".txt");
                List<SensorRecord> recs = new ArrayList<>();
                if (Files.exists(p)) {
                    for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                        if (!l.isBlank()) recs.add(SensorRecord.fromLine(l));
                    }
                }
                nodeRecs.add(recs);
                if (recs.size() < minLastSeq) minLastSeq = recs.size();
            }

            int diffs = 0;
            for (int s = 0; s < minLastSeq; s++) {
                String lineRef = nodeRecs.get(0).get(s).toLine();
                for (int i = 1; i < Wire.GRUPO; i++) {
                    if (!nodeRecs.get(i).get(s).toLine().equals(lineRef)) {
                        diffs++;
                        break;
                    }
                }
            }
            System.out.println("Partição p" + k + ": " + diffs + " divergências até ao menor seq (" + minLastSeq + ").");
        }
        System.out.println("=================================================");
    }

    private static long extractKeyLong(String[] e, String key) {
        for (String item : e) {
            if (item.startsWith(key + "=")) {
                return Long.parseLong(item.split("=")[1].replaceAll("[^0-9]", ""));
            }
        }
        return 0;
    }

    private static int extractNodeFromDetail(String[] e) {
        for (String item : e) {
            if (item.contains("n")) {
                String[] parts = item.split("n");
                if (parts.length > 1) {
                    try { return Integer.parseInt(parts[1].replaceAll("[^0-9]", "")); } catch (Exception ignored) {}
                }
            }
        }
        return 1;
    }

    private static int extractNodeFromFilename(String[] e) {
        return 1;
    }
}