package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class AlarmProcessor {

    private final int n;
    private final IpfsPubSub ps;
    private final Partitioner partitioner;
    private long lamport = 0;
    private final VectorClock vector;
    private final Set<String> processados = new HashSet<>();

    // COMINDIR04: Monitores do Processador
    private final Map<Integer, HeartbeatMonitor> monitores = new ConcurrentHashMap<>();
    private final Path monitorLog = Paths.get("monitor-processador.txt");
    private final ScheduledExecutorService detectorScheduler = Executors.newSingleThreadScheduledExecutor();

    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.ps = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);
        this.vector = new VectorClock(n);

        for (int k = 0; k < n; k++) {
            monitores.put(k, new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, this::registarEventoMonitor));
        }

        detectorScheduler.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores.values()) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private synchronized void registarEventoMonitor(String detalhe) {
        long agora = System.currentTimeMillis();
        String linha = agora + ";" + detalhe + "\n";
        System.out.print("[MONITOR PROC] " + linha);
        try {
            Files.writeString(monitorLog, linha, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public void iniciar() {
        System.out.println("Processador de alarmes iniciado (N=" + n + ")");
        for (int k = 0; k < n; k++) {
            final int particao = k;
            Thread t = new Thread(() -> escutarParticao(particao));
            t.setName("alarm-p" + particao);
            t.start();
        }
    }

    private void escutarParticao(int k) {
        String topico = Wire.topicData(k);
        System.out.println("Processador à escuta de " + topico);

        try {
            ps.subscribe(topico, mensagem -> {
                try {
                    processarMensagem(k, mensagem);
                } catch (Exception e) {
                    System.out.println("Erro ao processar mensagem de p" + k + ": " + e.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("Falha na subscrição de " + topico + ": " + e.getMessage());
        }
    }

    private void processarMensagem(int k, String mensagem) throws Exception {
        Batch batch;
        try {
            batch = Batch.decode(mensagem);
        } catch (Exception e) {
            return;
        }

        HeartbeatMonitor mon = monitores.get(k);
        if (mon != null) {
            mon.onHeartbeat(batch.getInc(), batch.getHb());
        }

        // Processa cada registo de dados acumulado no lote
        for (SensorRecord registo : batch.getRecords()) {
            if (registo.getSensorId().startsWith("ALARM-")) {
                continue;
            }

            String chave = k + ":" + registo.getSeq();
            long novoLamport;
            VectorClock novoVector;

            synchronized (this) {
                if (processados.contains(chave)) {
                    continue;
                }
                processados.add(chave);

                lamport = Math.max(lamport, registo.getLamport()) + 1;
                vector.merge(registo.getVector());

                novoLamport = lamport;
                novoVector = vector.copy();
            }

            if (registo.getTemperature() >= Wire.LIMIAR_ALARME) {
                String sensorAlarme = "ALARM-" + registo.getSensorId();
                int destino = partitioner.of(sensorAlarme);

                // Avisar se o destino estiver em suspeita
                HeartbeatMonitor destMon = monitores.get(destino);
                if (destMon != null && destMon.state() == HeartbeatMonitor.State.SUSPEITO) {
                    System.out.printf("AVISO: líder p%d suspeito; alarme %s pode perder-se%n", destino, sensorAlarme);
                }

                String msgAlarme = Wire.in(
                        sensorAlarme,
                        registo.getTemperature(),
                        novoLamport,
                        novoVector
                );

                ps.publish(Wire.topicIn(destino), msgAlarme);

                System.out.println("ALARME: " + registo.getSensorId() + " p" + k + "#"
                        + registo.getSeq() + " -> p" + destino);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = (args.length >= 2) ? args[1] : "http://127.0.0.1:5001/api/v0";

        AlarmProcessor processor = new AlarmProcessor(n, api);
        processor.iniciar();
    }
}