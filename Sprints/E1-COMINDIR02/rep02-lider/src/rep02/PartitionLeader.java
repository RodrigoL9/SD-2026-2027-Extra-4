package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    private final int k;
    private final int n;
    private final String enderecoAnunciado;
    private final int portoRecuperacao;
    private final RecordFile file;
    private final IpfsPubSub ps;
    private final Partitioner partitioner;
    private long ultimoSeq;

    public PartitionLeader(int k, int n, String endereco, String api) throws IOException {
        this.k = k;
        this.n = n;
        this.enderecoAnunciado = endereco;
        this.portoRecuperacao = Wire.PORTO_BASE + k;
        this.file = new RecordFile("lider-p" + k + ".txt");
        this.ps = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);
        this.ultimoSeq = this.file.lastSeq();
    }

    public void start() {
        // Thread 1: Entrada de Mensagens pelo PubSub
        Thread threadEntrada = new Thread(this::escutarEntrada, "Thread-Entrada-p" + k);
        threadEntrada.start();

        // Thread Principal: Servidor TCP de Recuperação
        servidorRecuperacao();
    }

    private void escutarEntrada() {
        String topicIn = Wire.topicIn(k);
        System.out.println("Líder p" + k + " à escuta em " + topicIn + " | Seq inicial: " + ultimoSeq);

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
            if (parts.length != 2) {
                System.out.println("REJEITADO: " + msg);
                return;
            }

            String sensor = parts[0];
            double temp = Double.parseDouble(parts[1]);

            // Validação de partição (Parte B)
            int partEsperada = partitioner.of(sensor);
            if (partEsperada != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + partEsperada);
                return;
            }

            SensorRecord record;
            synchronized (this) {
                ultimoSeq++;
                record = new SensorRecord(ultimoSeq, sensor, temp, java.time.Instant.now().toString());
                file.append(record);
            }

            String regMsg = Wire.reg(enderecoAnunciado, portoRecuperacao, record);
            ps.publish(Wire.topicData(k), regMsg);
            System.out.println("Disseminado [p" + k + "]: " + record.toLine());

        } catch (Exception e) {
            System.out.println("REJEITADO: " + msg);
        }
    }

    private void servidorRecuperacao() {
        try (ServerSocket server = new ServerSocket(portoRecuperacao)) {
            System.out.println("Servidor TCP de recuperação ativo no porto " + portoRecuperacao);

            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

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

                    System.out.println("RECUPERAÇÃO: " + de + ".." + ateAjustado + " para " + s.getRemoteSocketAddress() + " (" + registos.size() + " registos)");

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