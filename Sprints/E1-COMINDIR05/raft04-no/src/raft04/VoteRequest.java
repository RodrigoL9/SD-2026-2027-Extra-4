package raft04;

public final class VoteRequest {
    private final long term;
    private final int candidate;
    private final long lastSeq;
    private final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return String.format("PEDIDO_VOTO;%d;%d;%d;%d", term, candidate, lastSeq, lastTerm);
    }

    public static VoteRequest decode(String msg) {
        String[] p = msg.split(";");
        if (!"PEDIDO_VOTO".equals(p[0]) || p.length < 5) {
            throw new IllegalArgumentException("Mensagem PEDIDO_VOTO inválida: " + msg);
        }
        return new VoteRequest(Long.parseLong(p[1]), Integer.parseInt(p[2]), Long.parseLong(p[3]), Long.parseLong(p[4]));
    }

    public long getTerm() { return term; }
    public int getCandidate() { return candidate; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
}