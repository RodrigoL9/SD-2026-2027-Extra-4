package rep04;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class PartitionCheck {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }

        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        Partitioner partitioner = new Partitioner(n);

        System.out.println("==================================================");
        System.out.println("   RELATÓRIO DE AUDITORIA DE PARTIÇÕES (N = " + n + ")");
        System.out.println("==================================================");

        long totalGeralRegistosLideres = 0;

        for (int k = 0; k < n; k++) {
            System.out.println("\n--- Partição p" + k + " ---");
            Path pathLider = pasta.resolve("lider-p" + k + ".txt");

            if (!Files.exists(pathLider)) {
                System.out.println("AVISO: Ficheiro " + pathLider.getFileName() + " não encontrado.");
                continue;
            }

            List<SensorRecord> registosLider = carregarRegistos(pathLider);
            totalGeralRegistosLideres += registosLider.size();

            // 1. Validar sensores da partição
            boolean sensoresValidos = true;
            Set<String> sensoresDistintos = new HashSet<>();
            for (SensorRecord r : registosLider) {
                sensoresDistintos.add(r.getSensorId());
                if (partitioner.of(r.getSensorId()) != k) {
                    sensoresValidos = false;
                    System.out.println("ERRO: Sensor " + r.getSensorId() + " não pertence à partição " + k);
                }
            }
            if (sensoresValidos) {
                System.out.println("✓ Sensores: Todos os registos pertencem à partição p" + k);
            }

            // 2. Validar sequência do líder
            boolean seqValida = validarSequencia(registosLider);
            if (seqValida) {
                System.out.println("✓ Sequência líder: Inicia em 1, estritamente contínua sem saltos/duplicados.");
            } else {
                System.out.println("ERRO: Sequência do líder contém saltos ou duplicados!");
            }

            System.out.println("  Total registos líder: " + registosLider.size());
            System.out.println("  Sensores distintos: " + sensoresDistintos.size() + " " + sensoresDistintos);

            // 3. Procurar réplicas da partição k
            File pastaDir = pasta.toFile();
            final int partAtual = k;
            File[] ficheirosReplica = pastaDir.listFiles((dir, name) ->
                    name.matches("replica-\\d+-p" + partAtual + "\\.txt")
            );

            int numReplicas = (ficheirosReplica != null) ? ficheirosReplica.length : 0;
            System.out.println("  Réplicas a guardar p" + k + ": " + numReplicas + " (Esperado R = 2)");

            if (ficheirosReplica != null) {
                for (File fRep : ficheirosReplica) {
                    List<SensorRecord> regRep = carregarRegistos(fRep.toPath());
                    boolean repSeqValida = validarSequencia(regRep);
                    int emFalta = Math.max(0, registosLider.size() - regRep.size());

                    boolean isPrefixoOuIgual = ePrefixoOuIgual(registosLider, regRep);

                    String estado = isPrefixoOuIgual
                            ? (emFalta == 0 ? "✓ 100% CONVERGIDO" : "⚠ PREFIXO VÁLIDO (" + emFalta + " registos em falta)")
                            : "✗ DIVERGENTE DO LÍDER";

                    System.out.println("    -> " + fRep.getName() + ": " + regRep.size() + " registos | " + estado);
                }
            }
        }

        System.out.println("\n==================================================");
        System.out.println("Total agregado de registos no sistema: " + totalGeralRegistosLideres);
        System.out.println("==================================================");
    }

    private static List<SensorRecord> carregarRegistos(Path path) {
        List<SensorRecord> lista = new ArrayList<>();
        try {
            for (String linha : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                if (!linha.isBlank()) {
                    lista.add(SensorRecord.fromLine(linha));
                }
            }
        } catch (IOException e) {
            System.out.println("Erro ao ler " + path + ": " + e.getMessage());
        }
        return lista;
    }

    private static boolean validarSequencia(List<SensorRecord> lista) {
        if (lista.isEmpty()) return true;
        long esperada = 1;
        for (SensorRecord r : lista) {
            if (r.getSeq() != esperada) {
                return false;
            }
            esperada++;
        }
        return true;
    }

    private static boolean ePrefixoOuIgual(List<SensorRecord> lider, List<SensorRecord> replica) {
        if (replica.size() > lider.size()) return false;
        for (int i = 0; i < replica.size(); i++) {
            if (!replica.get(i).toLine().equals(lider.get(i).toLine())) {
                return false;
            }
        }
        return true;
    }
}
