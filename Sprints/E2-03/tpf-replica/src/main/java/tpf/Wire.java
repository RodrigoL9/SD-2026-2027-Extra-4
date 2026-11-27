package tpf;

import java.util.List;

public final class Wire {

    public static final int PORTO = Integer.getInteger("tpf.porto", 8765);
    public static final long PERIODO_HB_MS = Long.getLong("tpf.periodo", 2000L);
    public static final long SUSPEITA_MS = Long.getLong("tpf.suspeita", 6000L);
    public static final long MORTA_MS = Long.getLong("tpf.morta", 10000L);
    public static final long TOKEN_VALIDADE_MS = Long.getLong("tpf.tokenValidade", 600000L);

    public static final int R = Integer.getInteger("tpf.r", 3);
    public static final int W_MIN = Integer.getInteger("tpf.wMin", 2);
    public static final long ACK_TIMEOUT_MS = Long.getLong("tpf.ackTimeout", 4000L);
    public static final int MAX_RONDAS = Integer.getInteger("tpf.maxRondas", 3);
    public static final long MARGEM_BYTES = Long.getLong("tpf.margem", 65536L);
    public static final double ALFA = Double.parseDouble(System.getProperty("tpf.alfa", "1.0"));

    public static final long BACKOFF_MIN_MS = Long.getLong("tpf.backoffMin", 1000L);
    public static final long BACKOFF_MAX_MS = Long.getLong("tpf.backoffMax", 30000L);

    public static final long SELAGEM_MS = Long.getLong("tpf.selagem", 5000L);
    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMax", 65536);
    public static final int PAD_BYTES = Integer.getInteger("tpf.pad", 4096);

    public static final String TOPICO_LIDER = "tpf-lider";
    public static final String TOPICO_CTL = "tpf-ctl";

    private Wire() {}

    public static int maioria(int r) {
        return (r / 2) + 1;
    }

    public static int quorumDuravel(int r) {
        return Math.max(W_MIN, maioria(r));
    }

    public static String topicReplica(String id) {
        return "tpf-r-" + id;
    }

    public static String encodeSub(String topico) {
        return "SUB;" + topico;
    }

    public static String encodePub(String topico, String mensagem) {
        return "PUB;" + topico + ";" + mensagem;
    }

    public static String encodeOlaPrimeiro(String replica, String chavePublicaB64, String token) {
        return "OLA;" + replica + ";" + chavePublicaB64 + ";" + token;
    }

    public static String encodeOla(String replica) {
        return "OLA;" + replica;
    }

    public static String encodeDesafio(String nonceB64) {
        return "DESAFIO;" + nonceB64;
    }

    public static String encodeProva(String replica, String assinaturaB64) {
        return "PROVA;" + replica + ";" + assinaturaB64;
    }

    public static String encodeHb(String replica, long usados, long quota) {
        return "HB;" + replica + ";" + usados + ";" + quota;
    }

    public static String encodeInventario(String replica, List<String> chunkIds) {
        return "INVENTARIO;" + replica + ";" + String.join(",", chunkIds);
    }

    public static String encodeStore(String chunkId, String hdrB64, String binB64) {
        return "STORE;" + chunkId + ";" + hdrB64 + ";" + binB64;
    }

    public static String encodeAck(String replica, String chunkId, String sha256) {
        return "ACK;" + replica + ";" + chunkId + ";" + sha256;
    }

    public static String[] decode(String raw) {
        return raw.split(";", -1);
    }
}