package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Batch {
    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = (records != null) ? records : Collections.emptyList();
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(";")
                .append(port).append(";")
                .append(inc).append(";")
                .append(hb).append(";")
                .append(lastSeq).append(";")
                .append(records.size());

        for (SensorRecord r : records) {
            sb.append("\n").append(r.toLine());
        }
        return sb.toString();
    }

    // Sobrecarga padrão com 1 argumento para uso geral
    public static Batch decode(String msg) {
        return decode(msg, 0);
    }

    // Descodificação validando que as linhas de dados coincidem com 'n' do cabeçalho
    public static Batch decode(String msg, int nParticoes) {
        String[] lines = msg.split("\r?\n");
        if (lines.length == 0 || lines[0].isBlank()) {
            throw new IllegalArgumentException("Mensagem LOTE vazia");
        }

        String[] header = lines[0].split(";");
        if (!"LOTE".equals(header[0]) || header.length < 7) {
            throw new IllegalArgumentException("Cabeçalho de LOTE inválido: " + lines[0]);
        }

        String address = header[1];
        int port = Integer.parseInt(header[2]);
        long inc = Long.parseLong(header[3]);
        long hb = Long.parseLong(header[4]);
        long lastSeq = Long.parseLong(header[5]);
        int n = Integer.parseInt(header[6]);

        // Verificação obrigatória do guião: total de linhas - cabeçalho == n
        if (lines.length - 1 != n) {
            throw new IllegalStateException("Discrepância no LOTE: esperado " + n + " registos, recebido " + (lines.length - 1));
        }

        List<SensorRecord> records = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            // Se o SensorRecord da tua base aceitar apenas 1 argumento:
            records.add(SensorRecord.fromLine(lines[i]));
        }

        return new Batch(address, port, inc, hb, lastSeq, records);
    }

    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getInc() { return inc; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public List<SensorRecord> getRecords() { return records; }
}