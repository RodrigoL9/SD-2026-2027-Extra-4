package rep03;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PartitionReplica {

    private final int id;
    private final String api;

    private final Double probPerda;
    private final Long descartarSeq;

    private boolean descartouSeqAlvo = false;

    private final Random rnd = new Random();

    public PartitionReplica(int id, String api) {

        this.id = id;
        this.api = api;

        String p = System.getProperty("perda");

        this.probPerda =
                (p != null)
                        ? Double.parseDouble(p)
                        : null;

        String d = System.getProperty("descartar");

        this.descartarSeq =
                (d != null)
                        ? Long.parseLong(d)
                        : null;
    }


    public void iniciarParticao(int k) {

        new Thread(
                () -> gerirParticao(k),
                "Replica-" + id + "-p" + k
        ).start();
    }


    private void gerirParticao(int k) {

        try {

            RecordFile file =
                    new RecordFile(
                            "replica-" + id +
                                    "-p" + k + ".txt"
                    );

            long ultimoSeq =
                    file.lastSeq();

            TreeMap<Long, SensorRecord> retidos =
                    new TreeMap<>();

            String[] liderHost =
                    new String[]{"127.0.0.1"};

            int[] liderPorto =
                    new int[]{
                            Wire.PORTO_BASE + k
                    };

            IpfsPubSub ps =
                    new IpfsPubSub(api);

            String topic =
                    Wire.topicData(k);

            System.out.println(
                    "Réplica " + id +
                            " subscreve " + topic +
                            " | Seq inicial: " +
                            ultimoSeq
            );

            ps.subscribe(
                    topic,
                    line -> {

                        synchronized (file) {

                            processarLinhaPubSub(
                                    k,
                                    line,
                                    file,
                                    retidos,
                                    liderHost,
                                    liderPorto
                            );
                        }
                    }
            );

        } catch (Exception e) {

            System.err.println(
                    "Erro na partição " +
                            k +
                            " da réplica " +
                            id +
                            ": " +
                            e.getMessage()
            );
        }
    }


    private void processarLinhaPubSub(
            int k,
            String line,
            RecordFile file,
            TreeMap<Long, SensorRecord> retidos,
            String[] liderHost,
            int[] liderPorto) {

        try {

            if (!Wire.type(line).equals("REG")) {
                return;
            }

            String[] parts =
                    Wire.regParts(line);

            String host =
                    parts[0];

            int port =
                    Integer.parseInt(parts[1]);

            SensorRecord r =
                    SensorRecord.fromLine(parts[2]);

            /*
             * Atualiza o endereço do líder.
             */
            liderHost[0] = host;
            liderPorto[0] = port;

            long seq =
                    r.getSeq();


            /*
             * Simulação de descarte forçado.
             *
             * Exemplo:
             * -Ddescartar=5
             */
            if (descartarSeq != null
                    && seq == descartarSeq
                    && !descartouSeqAlvo) {

                descartouSeqAlvo = true;

                System.out.println(
                        "DESCARTADO (simulação): " +
                                seq
                );

                return;
            }


            /*
             * Simulação de perda aleatória.
             *
             * Exemplo:
             * -Dperda=0.1
             */
            if (probPerda != null
                    && rnd.nextDouble() < probPerda) {

                System.out.println(
                        "DESCARTADO (simulação): " +
                                seq
                );

                return;
            }


            long ultimo =
                    file.lastSeq();


            /*
             * Ignorar duplicados.
             */
            if (seq <= ultimo
                    || retidos.containsKey(seq)) {

                System.out.println(
                        "DUPLICADO: " + seq
                );

                return;
            }


            /*
             * Registo seguinte esperado.
             */
            if (seq == ultimo + 1) {

                file.append(r);

                System.out.println(
                        "Aplicado [p" + k + "]: " +
                                r.toLine()
                );

                drenarRetidos(
                        k,
                        file,
                        retidos
                );

            } else {

                /*
                 * Existe uma lacuna.
                 */
                System.out.println(
                        "EM ESPERA [p" + k + "]: " +
                                seq +
                                " (falta o registo " +
                                (ultimo + 1) +
                                ")"
                );

                retidos.put(
                        seq,
                        r
                );


                /*
                 * Se o buffer crescer demasiado,
                 * recuperamos os registos em falta
                 * diretamente do líder por TCP.
                 */
                if (retidos.size()
                        > Wire.LIMITE_RETIDOS) {

                    recuperarPorTcp(
                            k,
                            file,
                            retidos,
                            liderHost[0],
                            liderPorto[0]
                    );
                }
            }

        } catch (Exception e) {

            System.out.println(
                    "REJEITADO: " + line
            );
        }
    }


    private void drenarRetidos(
            int k,
            RecordFile file,
            TreeMap<Long, SensorRecord> retidos)
            throws IOException {

        long ultimo =
                file.lastSeq();

        while (!retidos.isEmpty()
                && retidos.firstKey()
                == ultimo + 1) {

            SensorRecord prox =
                    retidos
                            .pollFirstEntry()
                            .getValue();

            file.append(prox);

            ultimo =
                    prox.getSeq();

            System.out.println(
                    "Aplicado do buffer [p" +
                            k +
                            "]: " +
                            prox.toLine()
            );
        }
    }


    private void recuperarPorTcp(
            int k,
            RecordFile file,
            TreeMap<Long, SensorRecord> retidos,
            String host,
            int port) {

        try {

            long de =
                    file.lastSeq() + 1;

            long ate =
                    retidos.firstKey() - 1;

            System.out.println(
                    "RECUPERAÇÃO: " +
                            de +
                            ".." +
                            ate +
                            " pedida a " +
                            host +
                            ":" +
                            port
            );


            try (Socket s = new Socket()) {

                s.connect(
                        new InetSocketAddress(
                                host,
                                port
                        ),
                        Wire.T_TCP_MS
                );

                s.setSoTimeout(
                        Wire.T_TCP_MS
                );


                PrintWriter out =
                        new PrintWriter(
                                new OutputStreamWriter(
                                        s.getOutputStream(),
                                        StandardCharsets.UTF_8
                                ),
                                true
                        );

                BufferedReader in =
                        new BufferedReader(
                                new InputStreamReader(
                                        s.getInputStream(),
                                        StandardCharsets.UTF_8
                                )
                        );


                out.println(
                        "PEDIDO " +
                                de +
                                " " +
                                ate
                );


                String resp;

                while ((resp = in.readLine())
                        != null) {

                    resp = resp.trim();


                    if (resp.equals("FIM")) {
                        break;
                    }


                    if (resp.startsWith("ERRO")) {

                        System.out.println(
                                "RECUPERAÇÃO FALHOU: " +
                                        resp
                        );

                        return;
                    }


                    SensorRecord r =
                            SensorRecord.fromLine(
                                    resp
                            );

                    long ultimo =
                            file.lastSeq();


                    if (r.getSeq()
                            == ultimo + 1) {

                        file.append(r);

                        System.out.println(
                                "RECUPERADO: " +
                                        r.getSeq()
                        );
                    }
                }


                /*
                 * Depois da recuperação,
                 * tentar aplicar os elementos
                 * que estavam retidos.
                 */
                drenarRetidos(
                        k,
                        file,
                        retidos
                );
            }

        } catch (Exception e) {

            System.out.println(
                    "RECUPERAÇÃO FALHOU: " +
                            e.getMessage()
            );
        }
    }


    public static void main(String[] args) {

        if (args.length < 2) {

            System.out.println(
                    "Uso: PartitionReplica " +
                            "<id> <partições> [portoApi]"
            );

            return;
        }


        int id =
                Integer.parseInt(args[0]);

        String[] parts =
                args[1].split(",");

        String apiPort =
                (args.length > 2)
                        ? args[2]
                        : "5001";

        String api =
                "http://127.0.0.1:" +
                        apiPort +
                        "/api/v0";


        PartitionReplica replica =
                new PartitionReplica(
                        id,
                        api
                );


        for (String pStr : parts) {

            replica.iniciarParticao(
                    Integer.parseInt(
                            pStr.trim()
                    )
            );
        }
    }
}