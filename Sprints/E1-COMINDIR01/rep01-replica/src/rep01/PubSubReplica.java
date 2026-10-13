package rep01;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";
        String api = "http://127.0.0.1:" + port + "/api/v0";

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        long[] ultimoSeq = new long[1];

        try {
            ultimoSeq[0] = file.lastSeq();
            IpfsPubSub pubsub = new IpfsPubSub(api);
            System.out.println("PubSub Réplica " + id + " ligada a " + api + " | Seq inicial: " + ultimoSeq[0]);

            pubsub.subscribe(TOPIC, line -> {
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    return;
                }

                long seq = r.getSeq();

                if (seq <= ultimoSeq[0] || retidos.containsKey(seq)) {
                    System.out.println("DUPLICADO: " + seq);
                    return;
                }

                if (seq > ultimoSeq[0] + 1) {
                    retidos.put(seq, r);
                    System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimoSeq[0] + 1) + ")");
                    return;
                }

                try {
                    file.append(r);
                    ultimoSeq[0]++;
                    System.out.println("Aplicado: " + r.toLine());

                    while (!retidos.isEmpty() && retidos.firstKey() == ultimoSeq[0] + 1) {
                        Map.Entry<Long, SensorRecord> entry = retidos.pollFirstEntry();
                        file.append(entry.getValue());
                        ultimoSeq[0]++;
                        System.out.println("Aplicado (retido): " + entry.getValue().toLine());
                    }
                } catch (IOException ex) {
                    System.err.println("Erro ao escrever: " + ex.getMessage());
                }
            });
        } catch (IOException | InterruptedException e) {
            System.out.println("Erro na réplica PubSub: " + e.getMessage());
        }
    }
}