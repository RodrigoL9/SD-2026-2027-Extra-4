package rep01;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubsub = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider.txt");

        try (Scanner sc = new Scanner(System.in)) {
            long seq = file.lastSeq();
            System.out.println("PubSub Líder pronto ligado a " + api + ". Formato: <sensor> <temp> | 'rajada <n>' | 'sair'");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                if (line.toLowerCase().startsWith("rajada ")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        try {
                            int n = Integer.parseInt(parts[1]);
                            long tInicio = System.currentTimeMillis();
                            for (int i = 0; i < n; i++) {
                                double temp = ThreadLocalRandom.current().nextDouble(15.0, 30.0);
                                SensorRecord r = SensorRecord.now(++seq, "SIM", Math.round(temp * 10.0) / 10.0);
                                file.append(r);
                                pubsub.publish(TOPIC, r.toLine());
                            }
                            long tFim = System.currentTimeMillis();
                            System.out.println("Rajada IPFS terminada (" + n + "). Início: " + tInicio + " ms | Fim: " + tFim + " ms");
                            continue;
                        } catch (NumberFormatException | InterruptedException e) {
                            System.out.println("Erro na rajada: " + e.getMessage());
                            continue;
                        }
                    }
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                try {
                    double temp = Double.parseDouble(p[1]);
                    SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                    file.append(r);
                    pubsub.publish(TOPIC, r.toLine());
                    System.out.println("Publicado: " + r.toLine());
                } catch (NumberFormatException | InterruptedException e) {
                    System.out.println("Erro ao publicar: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Falha IO: " + e.getMessage());
        }
    }
}