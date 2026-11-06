package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionReplica {

    private final int id;
    private final String api;
    private final Double probPerda;
    private final Long descartarSeq;
    private boolean descartouSeqAlvo = false;
    private final Random rnd = new Random();

    // COMINDIR04: Monitores e Logs
    private final Map<Integer, HeartbeatMonitor> monitores = new ConcurrentHashMap<>();
    private final Path monitorLog;
    private final ScheduledExecutorService detectorScheduler = Executors.newSingleThreadScheduledExecutor();

    public PartitionReplica(int id, String api) {
        this.id = id;
        this.api = api;
        this.monitorLog = Paths.get("monitor-replica" + id + ".txt");

        String p = System.getProperty("perda");
        this.probPerda = (p != null) ? Double.parseDouble(p) : null;

        String d = System.getProperty("descartar");
        this.descartarSeq = (d != null) ? Long.parseLong(d) : null;

        // Verificação periódica de timeouts do detetor de falhas
        detectorScheduler.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores.values()) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private synchronized void registarEventoMonitor(String detalhe) {
        long agora = System.currentTimeMillis();
        String linha = agora + ";" + detalhe + "\n";
        System.out.print("[MONITOR " + id + "] " + linha);
        try {
            Files.writeString(monitorLog, linha, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Erro ao escrever monitor log: " + e.getMessage());
        }
    }

    public void iniciarParticao(int k) {
        monitores.put(k, new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, this::registarEventoMonitor));
        new Thread(() -> gerirParticao(k), "Replica-" + id + "-p" + k).start();
    }

    private void gerirParticao(int k) {
        try {
            RecordFile file = new RecordFile("replica-" + id + "-p" + k + ".txt");
            long ultimoSeq = file.lastSeq();
            TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
            String[] liderHost = new String[]{"127.0.0.1"};
            int[] liderPorto = new int[]{Wire.PORTO_BASE + k};

            IpfsPubSub ps = new IpfsPubSub(api);
            String topic = Wire.topicData(k);

            System.out.println("Réplica " + id + " subscreve " + topic + " | Seq inicial: " + ultimoSeq);

            ps.subscribe(topic, line -> {
                synchronized (file) {
                    processarLotePubSub(k, line, file, retidos, liderHost, liderPorto);
                }
            });

        } catch (Exception e) {
            System.err.println("Erro na partição " + k + " da réplica " + id + ": " + e.getMessage());
        }
    }

    private void processarLotePubSub(
            int k,
            String payload,
            RecordFile file,
            TreeMap<Long, SensorRecord> retidos,
            String[] liderHost,
            int[] liderPorto) {

        try {
            Batch batch = Batch.decode(payload);
            HeartbeatMonitor monitor = monitores.get(k);

            if (monitor != null) {
                monitor.onHeartbeat(batch.getInc(), batch.getHb());
            }

            liderHost[0] = batch.getAddress();
            liderPorto[0] = batch.getPort();

            List<SensorRecord> registos = batch.getRecords();
            String seqsInfo = registos.isEmpty() ? "[]" : "[" + registos.get(0).getSeq() + "-" + registos.get(registos.size() - 1).getSeq() + "]";

            registarEventoMonitor(String.format("RECEBIDO;p%d;inc=%d;hb=%d;ultimoSeq=%d;n=%d;seqs=%s",
                    k, batch.getInc(), batch.getHb(), batch.getLastSeq(), registos.size(), seqsInfo));

            // Aplicar os registos do lote ordenadamente
            for (SensorRecord r : registos) {
                long seq = r.getSeq();

                if (descartarSeq != null && seq == descartarSeq && !descartouSeqAlvo) {
                    descartouSeqAlvo = true;
                    System.out.println("DESCARTADO (simulação): " + seq);
                    continue;
                }

                if (probPerda != null && rnd.nextDouble() < probPerda) {
                    System.out.println("DESCARTADO (simulação): " + seq);
                    continue;
                }

                long ultimo = file.lastSeq();
                if (seq <= ultimo || retidos.containsKey(seq)) {
                    continue;
                }

                if (seq == ultimo + 1) {
                    file.append(r);
                    drenarRetidos(k, file, retidos);
                } else {
                    retidos.put(seq, r);
                    if (retidos.size() > Wire.LIMITE_RETIDOS) {
                        long ate = retidos.firstKey() - 1;
                        recuperarPorTcp(k, file, retidos, liderHost[0], liderPorto[0], file.lastSeq() + 1, ate);
                    }
                }
            }

            // Deteção de registos em falta pelo ultimoSeq do cabeçalho
            long ultimoAplicado = file.lastSeq();
            if (ultimoAplicado < batch.getLastSeq()) {
                long de = ultimoAplicado + 1;
                long ate = batch.getLastSeq();
                registarEventoMonitor(String.format("RECUPERACAO;p%d;de=%d;ate=%d", k, de, ate));
                recuperarPorTcp(k, file, retidos, liderHost[0], liderPorto[0], de, ate);
            }

        } catch (Exception e) {
            System.out.println("REJEITADO (LOTE inválido): " + e.getMessage());
        }
    }

    private void drenarRetidos(int k, RecordFile file, TreeMap<Long, SensorRecord> retidos) throws IOException {
        long ultimo = file.lastSeq();
        while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
            SensorRecord prox = retidos.pollFirstEntry().getValue();
            file.append(prox);
            ultimo = prox.getSeq();
        }
    }

    private void recuperarPorTcp(
            int k,
            RecordFile file,
            TreeMap<Long, SensorRecord> retidos,
            String host,
            int port,
            long de,
            long ate) {

        if (de > ate) return;

        System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + host + ":" + port);

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);

            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));

            out.println("PEDIDO " + de + " " + ate);

            String resp;
            while ((resp = in.readLine()) != null) {
                resp = resp.trim();
                if (resp.equals("FIM")) break;
                if (resp.startsWith("ERRO")) {
                    System.out.println("RECUPERAÇÃO FALHOU: " + resp);
                    return;
                }

                SensorRecord r = SensorRecord.fromLine(resp);
                if (r.getSeq() == file.lastSeq() + 1) {
                    file.append(r);
                    System.out.println("RECUPERADO: " + r.getSeq());
                }
            }
            drenarRetidos(k, file, retidos);

        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições> [portoApi]");
            return;
        }

        int id = Integer.parseInt(args[0]);
        String[] parts = args[1].split(",");
        String apiPort = (args.length > 2) ? args[2] : "5001";
        String api = "http://127.0.0.1:" + apiPort + "/api/v0";

        PartitionReplica replica = new PartitionReplica(id, api);
        for (String pStr : parts) {
            replica.iniciarParticao(Integer.parseInt(pStr.trim()));
        }
    }
}