package raft04;

public final class VoteReply {
    private final long term;
    private final int voter;
    private final int candidate;
    private final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return String.format("VOTO;%d;%d;%d;%s", term, voter, candidate, granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        String[] p = msg.split(";");
        if (!"VOTO".equals(p[0]) || p.length < 5) {
            throw new IllegalArgumentException("Mensagem VOTO inválida: " + msg);
        }
        return new VoteReply(Long.parseLong(p[1]), Integer.parseInt(p[2]), Integer.parseInt(p[3]), "S".equalsIgnoreCase(p[4]));
    }

    public long getTerm() { return term; }
    public int getVoter() { return voter; }
    public int getCandidate() { return candidate; }
    public boolean isGranted() { return granted; }
}