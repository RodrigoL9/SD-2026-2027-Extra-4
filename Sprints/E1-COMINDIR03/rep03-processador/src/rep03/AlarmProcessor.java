package rep03;

import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {

    private final int n;
    private final IpfsPubSub ps;
    private final Partitioner partitioner;

    // Relógios do processador
    private long lamport = 0;
    private final VectorClock vector;

    // Guarda "particao:seq" para ignorar duplicados
    private final Set<String> processados = new HashSet<>();

    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.ps = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);
        this.vector = new VectorClock(n);
    }

    public void iniciar() {
        System.out.println("Processador de alarmes iniciado (N=" + n + ")");

        // Uma thread por partição
        for (int k = 0; k < n; k++) {
            final int particao = k;

            Thread t = new Thread(() -> escutarParticao(particao));
            t.setName("alarm-p" + particao);
            t.start();
        }
    }

    private void escutarParticao(int k) {
        String topico = Wire.topicData(k);

        System.out.println(
                "Processador à escuta de " + topico
        );

        try {
            ps.subscribe(topico, mensagem -> {
                try {
                    processarMensagem(k, mensagem);
                } catch (Exception e) {
                    System.out.println(
                            "Erro ao processar mensagem de p" + k +
                                    ": " + e.getMessage()
                    );
                }
            });
        } catch (Exception e) {
            System.out.println(
                    "Falha na subscrição de " + topico +
                            ": " + e.getMessage()
            );
        }
    }

    private void processarMensagem(int k, String mensagem) throws Exception {

        // O processador recebe os REG publicados pelos líderes
        if (!Wire.type(mensagem).equals("REG")) {
            return;
        }

        String[] partes = Wire.regParts(mensagem);

        SensorRecord registo = SensorRecord.fromLine(partes[2]);

        /*
         * Os alarmes também passam pelos líderes.
         * Não podemos gerar um alarme a partir de outro alarme,
         * senão poderíamos criar um ciclo infinito.
         */
        if (registo.getSensorId().startsWith("ALARM-")) {
            return;
        }

        String chave = k + ":" + registo.getSeq();

        long novoLamport;
        VectorClock novoVector;

        /*
         * Os relógios e o conjunto de duplicados são partilhados
         * pelas threads que escutam as diferentes partições.
         */
        synchronized (this) {

            if (processados.contains(chave)) {
                return;
            }

            processados.add(chave);

            // Receção de um evento:
            // Lp = max(Lp, Lrecebido) + 1
            lamport = Math.max(
                    lamport,
                    registo.getLamport()
            ) + 1;

            // Vp = max(Vp, Vrecebido)
            vector.merge(registo.getVector());

            novoLamport = lamport;
            novoVector = vector.copy();
        }

        // Só gera alarme se atingir o limiar definido
        if (registo.getTemperature() >= Wire.LIMIAR_ALARME) {

            String sensorAlarme =
                    "ALARM-" + registo.getSensorId();

            int destino =
                    partitioner.of(sensorAlarme);

            String msgAlarme = Wire.in(
                    sensorAlarme,
                    registo.getTemperature(),
                    novoLamport,
                    novoVector
            );

            ps.publish(
                    Wire.topicIn(destino),
                    msgAlarme
            );

            System.out.println(
                    "ALARME: " +
                            registo.getSensorId() +
                            " p" + k +
                            "#" + registo.getSeq() +
                            " -> p" + destino
            );
        }
    }

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println(
                    "Uso: AlarmProcessor <N> [api]"
            );
            return;
        }

        int n = Integer.parseInt(args[0]);

        String api =
                "http://127.0.0.1:5001/api/v0";

        if (args.length >= 2) {
            api = args[1];
        }

        AlarmProcessor processor =
                new AlarmProcessor(n, api);

        processor.iniciar();
    }
}