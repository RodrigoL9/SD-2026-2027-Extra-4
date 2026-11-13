package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Batch {
    private final long term;
    private final int leader;
    private final String address;
    private final int port;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;
    private final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = (records != null) ? records : Collections.emptyList();
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(term).append(";")
                .append(leader).append(";")
                .append(address).append(";")
                .append(port).append(";")
                .append(hb).append(";")
                .append(lastSeq).append(";")
                .append(lastTerm).append(";")
                .append(records.size());

        for (SensorRecord r : records) {
            sb.append("\n").append(r.toLine());
        }
        return sb.toString();
    }

    public static Batch decode(String msg) {
        String[] lines = msg.split("\r?\n");
        if (lines.length == 0 || lines[0].isBlank()) {
            throw new IllegalArgumentException("LOTE vazio");
        }
        String[] header = lines[0].split(";");
        if (!"LOTE".equals(header[0]) || header.length < 9) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido: " + lines[0]);
        }

        long term = Long.parseLong(header[1]);
        int leader = Integer.parseInt(header[2]);
        String address = header[3];
        int port = Integer.parseInt(header[4]);
        long hb = Long.parseLong(header[5]);
        long lastSeq = Long.parseLong(header[6]);
        long lastTerm = Long.parseLong(header[7]);
        int n = Integer.parseInt(header[8]);

        List<SensorRecord> records = new ArrayList<>(n);
        for (int i = 1; i <= n && i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                records.add(SensorRecord.fromLine(lines[i]));
            }
        }
        return new Batch(term, leader, address, port, hb, lastSeq, lastTerm, records);
    }

    public long getTerm() { return term; }
    public long getTermo() { return term; }
    public long getInc() { return term; } // Alias de compatibilidade com COMINDIR04

    public int getLeader() { return leader; }
    public int getLider() { return leader; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
    public List<SensorRecord> getRecords() { return records; }
}