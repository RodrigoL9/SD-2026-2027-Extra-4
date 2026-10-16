package rep01;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> retidos = new TreeMap<>();

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);

            long ultimoSeq = file.lastSeq();
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT + " | Seq inicial: " + ultimoSeq);

            byte[] buffer = new byte[1000];
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p);

                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    continue;
                }

                long seq = r.getSeq();

                if (seq <= ultimoSeq || retidos.containsKey(seq)) {
                    System.out.println("DUPLICADO: " + seq);
                    continue;
                }

                if (seq > ultimoSeq + 1) {
                    retidos.put(seq, r);
                    System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimoSeq + 1) + ")");
                    continue;
                }

                file.append(r);
                ultimoSeq++;
                System.out.println("Aplicado: " + r.toLine());

                // Despeja os retidos que já têm sequência contígua
                while (!retidos.isEmpty() && retidos.firstKey() == ultimoSeq + 1) {
                    Map.Entry<Long, SensorRecord> entry = retidos.pollFirstEntry();
                    file.append(entry.getValue());
                    ultimoSeq++;
                    System.out.println("Aplicado (retido): " + entry.getValue().toLine());
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}