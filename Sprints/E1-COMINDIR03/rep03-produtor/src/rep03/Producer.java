package rep03;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;

public class Producer {

    public static void main(String[] args) {

        int n = 1;
        String api = "http://127.0.0.1:5001/api/v0";

        if (args.length >= 1) {
            try {
                n = Integer.parseInt(args[0]);

                if (args.length >= 2) {
                    api = args[1];
                }

            } catch (NumberFormatException e) {
                api = args[0];
            }
        }

        IpfsPubSub ps = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(n);

        System.out.println("Produtor iniciado (N=" + n + ") ligado a " + api);
        System.out.println(
                "Comandos: <sensor> <temperatura> | " +
                        "rajada <n> <s> | rajada-quente <n> <s> | sair"
        );

        try (BufferedReader br =
                     new BufferedReader(new InputStreamReader(System.in))) {

            String line;

            while ((line = br.readLine()) != null) {

                line = line.trim();

                if (line.equalsIgnoreCase("sair")) {
                    break;
                }

                if (line.isBlank()) {
                    continue;
                }

                if (line.startsWith("rajada ")
                        || line.startsWith("rajada-quente ")) {

                    executarRajada(ps, partitioner, line);

                } else {

                    String[] parts = line.split("\\s+");

                    if (parts.length == 2) {

                        try {
                            String sensor = parts[0];
                            double temp = Double.parseDouble(parts[1]);

                            int particao = partitioner.of(sensor);

                            // O produtor não possui conhecimento causal:
                            // L = 0 e V = (0,...,0)
                            VectorClock vector = new VectorClock(n);

                            ps.publish(
                                    Wire.topicIn(particao),
                                    Wire.in(sensor, temp, 0, vector)
                            );

                            System.out.println(
                                    "Publicado em "
                                            + Wire.topicIn(particao)
                                            + " -> p"
                                            + particao
                            );

                        } catch (NumberFormatException e) {
                            System.out.println("Temperatura inválida.");

                        } catch (Exception e) {
                            System.out.println(
                                    "Erro ao publicar: " + e.getMessage()
                            );
                        }

                    } else {
                        System.out.println(
                                "Formato: <sensor> <temperatura> " +
                                        "(ex: S01 22.4)"
                        );
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void executarRajada(
            IpfsPubSub ps,
            Partitioner partitioner,
            String cmd) {

        String[] parts = cmd.split("\\s+");

        if (parts.length != 3) {
            System.out.println(
                    "Uso: rajada <n> <s> ou rajada-quente <n> <s>"
            );
            return;
        }

        boolean quente =
                parts[0].equalsIgnoreCase("rajada-quente");

        int nMsgs = Integer.parseInt(parts[1]);
        int maxSensores = Integer.parseInt(parts[2]);

        Random rnd = new Random();

        int[] contagemParticoes =
                new int[partitioner.count()];

        long tInicio = System.currentTimeMillis();

        for (int i = 0; i < nMsgs; i++) {

            String sensor;

            if (quente && rnd.nextDouble() < 0.8) {
                sensor = "S01";

            } else {
                int sNum =
                        1 + rnd.nextInt(maxSensores);

                sensor =
                        String.format("S%02d", sNum);
            }

            double temp =
                    15.0 + (15.0 * rnd.nextDouble());

            temp =
                    Math.round(temp * 10.0) / 10.0;

            int particao =
                    partitioner.of(sensor);

            contagemParticoes[particao]++;

            try {

                // Também nas rajadas o produtor envia
                // L = 0 e V = (0,...,0)
                VectorClock vector =
                        new VectorClock(partitioner.count());

                ps.publish(
                        Wire.topicIn(particao),
                        Wire.in(sensor, temp, 0, vector)
                );

                Thread.sleep(2);

            } catch (Exception e) {
                System.out.println(
                        "Erro no envio: " + e.getMessage()
                );
            }
        }

        long tFim =
                System.currentTimeMillis();

        System.out.println(
                "Rajada terminada (" + nMsgs + "). "
                        + "Início: " + tInicio
                        + " | Fim: " + tFim
                        + " | Delta: "
                        + (tFim - tInicio)
                        + " ms"
        );

        for (int k = 0;
             k < contagemParticoes.length;
             k++) {

            System.out.println(
                    "  -> p"
                            + k
                            + ": "
                            + contagemParticoes[k]
                            + " leituras"
            );
        }
    }
}