package rep04;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class Producer {

    private static final Map<Integer, HeartbeatMonitor> monitores = new ConcurrentHashMap<>();
    private static final Path monitorLog = Paths.get("monitor-produtor.txt");
    private static final ScheduledExecutorService detectorScheduler = Executors.newSingleThreadScheduledExecutor();

    private static synchronized void registarEventoMonitor(String detalhe) {
        long agora = System.currentTimeMillis();
        String linha = agora + ";" + detalhe + "\n";
        System.out.print("[MONITOR PROD] " + linha);
        try {
            Files.writeString(monitorLog, linha, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) {
        int n = 1;
        String api = "http://127.0.0.1:5001/api/v0";

        if (args.length >= 1) {
            try {
                n = Integer.parseInt(args[0]);
                if (args.length >= 2) api = args[1];
            } catch (NumberFormatException e) {
                api = args[0];
            }
        }

        IpfsPubSub ps = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(n);

        // Configuração dos monitores e subscrição de cada partição
        for (int k = 0; k < n; k++) {
            final int part = k;
            HeartbeatMonitor mon = new HeartbeatMonitor(part, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, Producer::registarEventoMonitor);
            monitores.put(part, mon);

            new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(part), payload -> {
                        try {
                            Batch b = Batch.decode(payload);
                            mon.onHeartbeat(b.getInc(), b.getHb());
                        } catch (Exception ignored) {}
                    });
                } catch (Exception e) {
                    System.err.println("Erro ao subscrever topicData(" + part + "): " + e.getMessage());
                }
            }, "MonitorPubSub-p" + part).start();
        }

        detectorScheduler.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores.values()) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        System.out.println("Produtor iniciado (N=" + n + ") ligado a " + api);
        System.out.println("Comandos: <sensor> <temperatura> | rajada <n> <s> | rajada-quente <n> <s> | sair");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.equalsIgnoreCase("sair")) break;
                if (line.isBlank()) continue;

                if (line.startsWith("rajada ") || line.startsWith("rajada-quente ")) {
                    executarRajada(ps, partitioner, line, n);
                } else {
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        try {
                            String sensor = parts[0];
                            double temp = Double.parseDouble(parts[1]);
                            int particao = partitioner.of(sensor);

                            HeartbeatMonitor mon = monitores.get(particao);
                            if (mon != null && mon.state() == HeartbeatMonitor.State.SUSPEITO) {
                                System.out.printf("AVISO: líder p%d suspeito; leitura %s pode perder-se%n", particao, sensor);
                            }

                            VectorClock vector = new VectorClock(n);
                            ps.publish(Wire.topicIn(particao), Wire.in(sensor, temp, 0, vector));
                            System.out.println("Publicado em " + Wire.topicIn(particao) + " -> p" + particao);

                        } catch (NumberFormatException e) {
                            System.out.println("Temperatura inválida.");
                        } catch (Exception e) {
                            System.out.println("Erro ao publicar: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Formato: <sensor> <temperatura> (ex: S01 22.4)");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executarRajada(IpfsPubSub ps, Partitioner partitioner, String cmd, int n) {
        String[] parts = cmd.split("\\s+");
        if (parts.length != 3) {
            System.out.println("Uso: rajada <n> <s> ou rajada-quente <n> <s>");
            return;
        }

        boolean quente = parts[0].equalsIgnoreCase("rajada-quente");
        int nMsgs = Integer.parseInt(parts[1]);
        int maxSensores = Integer.parseInt(parts[2]);
        Random rnd = new Random();
        int[] contagemParticoes = new int[partitioner.count()];
        long tInicio = System.currentTimeMillis();

        for (int i = 0; i < nMsgs; i++) {
            String sensor;
            if (quente && rnd.nextDouble() < 0.8) {
                sensor = "S01";
            } else {
                int sNum = 1 + rnd.nextInt(maxSensores);
                sensor = String.format("S%02d", sNum);
            }

            double temp = Math.round((15.0 + (15.0 * rnd.nextDouble())) * 10.0) / 10.0;
            int particao = partitioner.of(sensor);
            contagemParticoes[particao]++;

            HeartbeatMonitor mon = monitores.get(particao);
            if (mon != null && mon.state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.printf("AVISO: líder p%d suspeito; leitura %s pode perder-se%n", particao, sensor);
            }

            try {
                VectorClock vector = new VectorClock(n);
                ps.publish(Wire.topicIn(particao), Wire.in(sensor, temp, 0, vector));
                Thread.sleep(2);
            } catch (Exception e) {
                System.out.println("Erro no envio: " + e.getMessage());
            }
        }

        long tFim = System.currentTimeMillis();
        System.out.println("Rajada terminada (" + nMsgs + "). Delta: " + (tFim - tInicio) + " ms");
        for (int k = 0; k < contagemParticoes.length; k++) {
            System.out.println("  -> p" + k + ": " + contagemParticoes[k] + " leituras");
        }
    }
}