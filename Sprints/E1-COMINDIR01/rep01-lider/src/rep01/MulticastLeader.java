package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {

            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1); // Não sai da rede local
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();

            System.out.println("Líder pronto. Formato: <sensor> <temp> | 'rajada <n>' | 'sair'");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                // Suporte a rajada rápida de testes (Ponto 8)
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
                                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                                socket.send(new DatagramPacket(m, m.length, group, PORT));
                            }
                            long tFim = System.currentTimeMillis();
                            System.out.println("Rajada enviada (" + n + " registos). Duração: " + (tFim - tInicio) + " ms");
                            continue;
                        } catch (NumberFormatException e) {
                            System.out.println("Número de rajada inválido.");
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
                    file.append(r); // 1. Escreve primeiro em disco local
                    byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                    socket.send(new DatagramPacket(m, m.length, group, PORT)); // 2. Propaga
                    System.out.println("Registado e enviado: " + r.toLine());
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida.");
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}