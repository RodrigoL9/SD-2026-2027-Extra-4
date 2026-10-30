package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {

    private final int k;
    private final int n;
    private final String enderecoAnunciado;
    private final int portoRecuperacao;
    private final RecordFile file;
    private final IpfsPubSub ps;
    private final Partitioner partitioner;

    private long ultimoSeq;

    // Relógios da COMINDIR03
    private long lamport;
    private VectorClock vector;

    // COMINDIR04: Estado do Lote e Heartbeat
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final List<SensorRecord> pendentes = new ArrayList<>();
    private final long inc = System.currentTimeMillis();
    private long hb = 0;
    private final Path caminhoHb;

    public PartitionLeader(int k, int n, String endereco, String api) throws IOException {
        this.k = k;
        this.n = n;
        this.enderecoAnunciado = endereco;
        this.portoRecuperacao = Wire.PORTO_BASE + k;
        this.file = new RecordFile("lider-p" + k + ".txt");
        this.caminhoHb = Paths.get("hb-p" + k + ".txt");
        this.ps = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);

        SensorRecord ultimo = this.file.last();
        if (ultimo == null) {
            this.ultimoSeq = 0;
            this.lamport = 0;
            this.vector = new VectorClock(n);
        } else {
            this.ultimoSeq = ultimo.getSeq();
            this.lamport = ultimo.getLamport();
            this.vector = ultimo.getVector().copy();
        }
    }

    public void start() {
        // Iniciar envio periódico de lotes/heartbeats a cada PERIODO_HB_MS
        scheduler.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        // Thread 1: entrada de mensagens pelo PubSub
        Thread threadEntrada = new Thread(this::escutarEntrada, "Thread-Entrada-p" + k);
        threadEntrada.start();

        // Thread principal: servidor TCP de recuperação
        servidorRecuperacao();
    }

    private void escutarEntrada() {
        String topicIn = Wire.topicIn(k);
        System.out.println("Líder p" + k + " à escuta em " + topicIn
                + " | Seq inicial: " + ultimoSeq + " | Inc: " + inc);

        try {
            ps.subscribe(topicIn, line -> {
                try {
                    processarMensagemEntrada(line);
                } catch (Exception e) {
                    System.out.println("Erro ao processar mensagem: " + e.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("Falha na subscrição de entrada: " + e.getMessage());
        }
    }

    private void processarMensagemEntrada(String msg) {
        try {
            String tipo = Wire.type(msg);
            if (!tipo.equals("IN")) {
                System.out.println("REJEITADO: " + msg);
                return;
            }

            String corpo = Wire.body(msg);
            String[] parts = corpo.split(";");
            if (parts.length != 4) {
                System.out.println("REJEITADO: " + msg);
                return;
            }

            String sensor = parts[0];
            double temp = Double.parseDouble(parts[1]);
            long lMsg = Long.parseLong(parts[2]);
            VectorClock vMsg = VectorClock.parse(parts[3], n);

            int partEsperada = partitioner.of(sensor);
            if (partEsperada != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + partEsperada);
                return;
            }

            // Atualização dos relógios, atribuição de seq e acumulação em lote na mesma secção sincronizada
            synchronized (this) {
                lamport = Math.max(lamport, lMsg) + 1;
                vector.merge(vMsg);
                vector.tick(k);
                ultimoSeq++;

                if (vector.get(k) != ultimoSeq) {
                    throw new IllegalStateException("V[" + k + "] = " + vector.get(k) + " mas seq = " + ultimoSeq);
                }

                SensorRecord record = new SensorRecord(
                        ultimoSeq,
                        sensor,
                        temp,
                        java.time.Instant.now().toString(),
                        lamport,
                        vector.copy()
                );

                file.append(record);
                pendentes.add(record); // Acumulado no lote para o próximo período

                System.out.println("EVENTO: seq=" + ultimoSeq + " L=" + lamport + " V=" + vector);
            }

        } catch (Exception e) {
            System.out.println("REJEITADO: " + msg + " | " + e.getMessage());
        }
    }

    private void enviarLote() {
        List<SensorRecord> loteEnvio;
        long hbEnvio;
        long seqEnvio;

        // Troca atómica de lista dentro do monitor
        synchronized (this) {
            hb++;
            hbEnvio = hb;
            seqEnvio = ultimoSeq;
            loteEnvio = new ArrayList<>(pendentes);
            pendentes.clear();
        }

        // Publicação e I/O fora da secção sincronizada
        try {
            Batch batch = new Batch(enderecoAnunciado, portoRecuperacao, inc, hbEnvio, seqEnvio, loteEnvio);
            String payload = batch.encode();
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);

            ps.publish(Wire.topicData(k), payload);

            // Registo em hb-p<k>.txt: inc;hb;instante;ultimoSeq;n;bytes
            String linhaHb = String.format("%d;%d;%d;%d;%d;%d\n",
                    inc, hbEnvio, System.currentTimeMillis(), seqEnvio, loteEnvio.size(), bytes.length);

            Files.writeString(caminhoHb, linhaHb, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            System.out.printf("HEARTBEAT: hb=%d n=%d ultimoSeq=%d%n", hbEnvio, loteEnvio.size(), seqEnvio);

        } catch (Exception e) {
            System.err.println("Erro na publicação do LOTE: " + e.getMessage());
        }
    }

    private void servidorRecuperacao() {
        try (ServerSocket server = new ServerSocket(portoRecuperacao)) {
            System.out.println("Servidor TCP de recuperação ativo no porto " + portoRecuperacao);

            while (true) {
                try (
                        Socket s = server.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)
                ) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();
                    if (pedido == null || !pedido.startsWith("PEDIDO ")) {
                        out.println("ERRO Pedido inválido");
                        continue;
                    }

                    String[] tokens = pedido.trim().split("\\s+");
                    if (tokens.length != 3) {
                        out.println("ERRO Formato esperado: PEDIDO <de> <até>");
                        continue;
                    }

                    long de = Long.parseLong(tokens[1]);
                    long ate = Long.parseLong(tokens[2]);

                    long maxSeqLocal;
                    synchronized (this) {
                        maxSeqLocal = ultimoSeq;
                    }

                    if (de > ate) {
                        out.println("ERRO Intervalo inválido: de > até");
                        continue;
                    }

                    long ateAjustado = Math.min(ate, maxSeqLocal);
                    List<SensorRecord> registos = file.range(de, ateAjustado);

                    for (SensorRecord r : registos) {
                        out.println(r.toLine());
                    }
                    out.println("FIM");

                    System.out.println("RECUPERAÇÃO: " + de + ".." + ateAjustado + " para "
                            + s.getRemoteSocketAddress() + " (" + registos.size() + " registos)");

                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                } catch (NumberFormatException e) {
                    System.out.println("Pedido com valores numéricos inválidos.");
                }
            }
        } catch (IOException e) {
            System.err.println("Erro no ServerSocket do líder p" + k + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        String endereco = (args.length > 2) ? args[2] : "127.0.0.1";
        String api = (args.length > 3) ? args[3] : "http://127.0.0.1:5001/api/v0";

        PartitionLeader leader = new PartitionLeader(k, n, endereco, api);
        leader.start();
    }
}