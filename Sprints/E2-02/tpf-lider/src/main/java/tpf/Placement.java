package tpf;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public final class Placement {

    // vista de uma réplica no instante da decisão
    public static final class Candidate {
        public final String id;
        public final long free;          // quota − usados − reservados, em bytes

        public Candidate(String id, long free) {
            this.id = id;
            this.free = free;
        }
    }

    private final double alpha;
    private final long margin;

    public Placement(double alpha, long margin) {
        this.alpha = alpha;
        this.margin = margin;
    }

    // u(c, r) uniforme em ]0, 1[, a partir dos primeiros 53 bits de SHA-256(c | r)
    static double unit(String chunkId, String replicaId) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256")
                    .digest((chunkId + "|" + replicaId).getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 8; i++) v = (v << 8) | (h[i] & 0xff);
            v >>>= 11;
            return (v + 0.5) / (double) (1L << 53);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    double score(String chunkId, Candidate c) {
        return Math.pow((double) c.free, alpha) / -Math.log(unit(chunkId, c.id));
    }

    // devolve até 'count' réplicas, por ordem decrescente de pontuação; pode devolver menos
    public List<String> choose(String chunkId, long size, int count,
                               Collection<Candidate> alive, Set<String> exclude) {
        List<Candidate> ok = new ArrayList<>();
        for (Candidate c : alive)
            if (!exclude.contains(c.id) && c.free >= size + margin) ok.add(c);
        ok.sort(Comparator.comparingDouble((Candidate c) -> score(chunkId, c)).reversed());
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(count, ok.size()); i++) out.add(ok.get(i).id);
        return out;
    }
}
