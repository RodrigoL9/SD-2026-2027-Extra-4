package rep04;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class HeartbeatCheck {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java rep04.HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        Path ficheiroFalhas = args.length >= 3 ? Paths.get(args[2]) : null;

        System.out.println("==================================================");
        System.out.println("   RELATÓRIO AUDITORIA HEARTBEAT (COMINDIR04)");
        System.out.println("==================================================");

        // 1. Métricas de hb-p<k>.txt por partição
        for (int k = 0; k < n; k++) {
            Path hbPath = pasta.resolve("hb-p" + k + ".txt");
            if (!Files.exists(hbPath)) continue;

            System.out.println("\n[PARTIÇÃO p" + k + "]");
            try {
                List<String> linhas = Files.readAllLines(hbPath, StandardCharsets.UTF_8);
                int totalLotes = 0;
                int lotesVazios = 0;
                long totalRegistos = 0;
                long maxRegistos = 0;
                long intervaloAcumulado = 0;
                long maxIntervalo = 0;
                long ultimoInstante = -1;
                long totalBytes = 0;

                for (String linha : linhas) {
                    if (linha.isBlank()) continue;
                    String[] campos = linha.split(";");
                    long instante = Long.parseLong(campos[2]);
                    long numRegs = Long.parseLong(campos[4]);
                    long bytes = Long.parseLong(campos[5]);

                    totalLotes++;
                    if (numRegs == 0) lotesVazios++;
                    totalRegistos += numRegs;
                    if (numRegs > maxRegistos) maxRegistos = numRegs;
                    totalBytes += bytes;

                    if (ultimoInstante != -1) {
                        long diff = instante - ultimoInstante;
                        intervaloAcumulado += diff;
                        if (diff > maxIntervalo) maxIntervalo = diff;
                    }
                    ultimoInstante = instante;
                }

                double propVazios = totalLotes > 0 ? (double) lotesVazios / totalLotes : 0.0;
                double mediaRegistos = totalLotes > 0 ? (double) totalRegistos / totalLotes : 0.0;
                double mediaIntervalo = totalLotes > 1 ? (double) intervaloAcumulado / (totalLotes - 1) : 0.0;
                double bytesPorReg = totalRegistos > 0 ? (double) totalBytes / totalRegistos : 0.0;

                System.out.printf("  Total de lotes: %d (Vazios: %.1f%%)\n", totalLotes, propVazios * 100);
                System.out.printf("  Registos/lote: Média = %.2f, Máximo = %d\n", mediaRegistos, maxRegistos);
                System.out.printf("  Intervalo entre heartbeats: Média = %.1f ms, Máx = %d ms\n", mediaIntervalo, maxIntervalo);
                System.out.printf("  Tamanho médio em LOTE: %.1f bytes/registo\n", bytesPorReg);
            } catch (IOException e) {
                System.err.println("Erro ao ler " + hbPath.getFileName() + ": " + e.getMessage());
            }
        }

        // 2. Análise dos ficheiros monitor-*.txt
        File[] ficheirosMonitor = pasta.toFile().listFiles((dir, name) -> name.startsWith("monitor-") && name.endsWith(".txt"));
        if (ficheirosMonitor != null && ficheirosMonitor.length > 0) {
            System.out.println("\n--- Eventos Registados nos Monitores ---");
            for (File fm : ficheirosMonitor) {
                System.out.println("\nFicheiro: " + fm.getName());
                try {
                    List<String> linhas = Files.readAllLines(fm.toPath(), StandardCharsets.UTF_8);
                    int suspeitas = 0;
                    int retomas = 0;
                    int reinicios = 0;
                    int hbPerdidos = 0;
                    int recuperacoes = 0;

                    for (String l : linhas) {
                        if (l.contains(";SUSPEITA;")) suspeitas++;
                        if (l.contains(";RETOMA;")) retomas++;
                        if (l.contains(";REINICIO;")) reinicios++;
                        if (l.contains(";HB_PERDIDO;")) hbPerdidos++;
                        if (l.contains(";RECUPERACAO;")) recuperacoes++;
                    }

                    System.out.printf("  SUSPEITA: %d | RETOMA: %d | REINICIO: %d\n", suspeitas, retomas, reinicios);
                    System.out.printf("  HB_PERDIDO: %d | RECUPERACAO: %d\n", hbPerdidos, recuperacoes);
                    System.out.printf("  Classificação: %s\n", (retomas > 0 ? "FALSAS SUSPEITAS detetadas (houve retoma)" : "Falhas reais (sem retoma da mesma incarnação)"));
                } catch (IOException e) {
                    System.err.println("Erro ao processar " + fm.getName());
                }
            }
        }

        // 3. Cruzamento com falhas.txt
        if (ficheiroFalhas != null && Files.exists(ficheiroFalhas)) {
            System.out.println("\n--- Tempos de Deteção das Falhas (falhas.txt) ---");
            try {
                for (String l : Files.readAllLines(ficheiroFalhas, StandardCharsets.UTF_8)) {
                    if (!l.isBlank()) System.out.println("  Instante de falha anotado: " + l);
                }
            } catch (IOException ignored) {}
        }
        System.out.println("\n==================================================");
    }
}