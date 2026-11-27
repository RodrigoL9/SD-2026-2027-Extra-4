package tpf;

import java.util.*;

public final class PlacementExp5 {
    public static void main(String[] args) {
        long mb = 1024L * 1024L;
        List<Placement.Candidate> candidates = List.of(
                new Placement.Candidate("A (8 MB)", 8 * mb),
                new Placement.Candidate("B (2 MB)", 2 * mb),
                new Placement.Candidate("C (4 MB)", 4 * mb),
                new Placement.Candidate("D (6 MB)", 6 * mb)
        );
        int totalChunks = 10_000;
        for (double alfa : new double[]{1.0, 0.0, 2.0}) {
            Placement placement = new Placement(alfa, Wire.MARGEM_BYTES);
            Map<String, Integer> counts = new LinkedHashMap<>();
            for (Placement.Candidate c : candidates) counts.put(c.id, 0);
            for (int i = 0; i < totalChunks; i++) {
                List<String> chosen = placement.choose("chunk-exp5-" + i, 4112L, 1, candidates, Set.of());
                if (!chosen.isEmpty()) counts.merge(chosen.get(0), 1, Integer::sum);
            }
            System.out.printf("=== Experiência 5 (alfa = %.1f) ===%n", alfa);
            counts.forEach((k, v) -> System.out.printf("  %-10s -> %4d (%.2f%%)%n", k, v, (100.0 * v) / totalChunks));
        }
    }
}