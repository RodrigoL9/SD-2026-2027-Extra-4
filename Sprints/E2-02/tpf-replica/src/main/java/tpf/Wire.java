package tpf;

public final class Wire {

    public static final int R = Integer.getInteger("tpf.r", 3);
    public static final int W = Integer.getInteger("tpf.w", 2);
    public static final double ALFA = Double.parseDouble(System.getProperty("tpf.alfa", "1.0"));
    public static final long PERIODO_HB_MS = Long.getLong("tpf.periodo", 2000L);
    public static final long MORTA_MS = Long.getLong("tpf.morta", 30000L);
    public static final long RECONCILIACAO_MS = Long.getLong("tpf.reconciliacao", 5000L);
    public static final long SELAGEM_MS = Long.getLong("tpf.selagem", 5000L);
    public static final long ACK_TIMEOUT_MS = Long.getLong("tpf.ackTimeout", 4000L);
    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMax", 65536);
    public static final int PAD_BYTES = Integer.getInteger("tpf.pad", 4096);
    public static final long MARGEM_BYTES = Long.getLong("tpf.margem", 65536L);
    public static final long MANIFESTO_MS = Long.getLong("tpf.manifesto", 10000L);
    public static final int PORTO = Integer.getInteger("tpf.porto", 8765);
    public static final int REPARACOES = Integer.getInteger("tpf.reparacoes", 20);
    public static final long ESPERA_MANIFESTO_MS = Long.getLong("tpf.esperaManifesto", 10000L);

    public static final String TOPICO_LIDER = "tpf-lider";
    public static final String TOPICO_CTL = "tpf-ctl";

    private Wire() {}

    public static String topicReplica(String id) {
        return "tpf-r-" + id;
    }

    public static String encodeSub(String topico) {
        return "SUB;" + topico;
    }

    public static String encodePub(String topico, String mensagem) {
        return "PUB;" + topico + ";" + mensagem;
    }

    public static String encodeOla(String replica, String chavePublicaB64, int versao, String token) {
        if (token != null && !token.isBlank()) {
            return "OLA;" + replica + ";" + chavePublicaB64 + ";" + versao + ";" + token;
        }
        return "OLA;" + replica + ";" + chavePublicaB64 + ";" + versao;
    }

    public static String encodeDesafio(String nonceB64) {
        return "DESAFIO;" + nonceB64;
    }

    public static String encodeProva(String replica, String assinaturaB64) {
        return "PROVA;" + replica + ";" + assinaturaB64;
    }

    public static String encodeHb(String replica, long hb, long quota, long usados, int nChunks, int versaoManifesto) {
        return "HB;" + replica + ";" + hb + ";" + quota + ";" + usados + ";" + nChunks + ";" + versaoManifesto;
    }

    public static String encodeStore(String chunkId, int versaoChave, String saltB64, String nonceB64, int tamanho, String dadosB64) {
        return "STORE;" + chunkId + ";" + versaoChave + ";" + saltB64 + ";" + nonceB64 + ";" + tamanho + ";" + dadosB64;
    }

    public static String encodeAck(String replica, String chunkId, String sha256) {
        return "ACK;" + replica + ";" + chunkId + ";" + sha256;
    }

    public static String encodeFetch(String pedido, String chunkId) {
        return "FETCH;" + pedido + ";" + chunkId;
    }

    public static String encodeData(String replica, String pedido, String chunkId, int versaoChave, String saltB64, String nonceB64, String dadosB64) {
        return "DATA;" + replica + ";" + pedido + ";" + chunkId + ";" + versaoChave + ";" + saltB64 + ";" + nonceB64 + ";" + dadosB64;
    }

    public static String encodeDelete(String chunkId) {
        return "DELETE;" + chunkId;
    }

    public static String encodeInventarioReq(String pedido) {
        return "INVENTARIO?;" + pedido;
    }

    public static String encodeInventario(String replica, String pedido, java.util.List<String> chunkIds) {
        return "INVENTARIO;" + replica + ";" + pedido + ";" + chunkIds.size() + ";" + String.join(",", chunkIds);
    }

    public static String encodeManifesto(int versao, String chunkId, int versaoChave, String saltB64, String nonceB64, String dadosB64) {
        return "MANIFESTO;" + versao + ";" + chunkId + ";" + versaoChave + ";" + saltB64 + ";" + nonceB64 + ";" + dadosB64;
    }

    public static String encodeManifestoReq(String pedido) {
        return "MANIFESTO?;" + pedido;
    }

    public static String encodePosseReq(String pedido, String chunkId, int inicio, int comprimento, String nonceB64) {
        return "POSSE?;" + pedido + ";" + chunkId + ";" + inicio + ";" + comprimento + ";" + nonceB64;
    }

    public static String encodePosse(String replica, String pedido, String sha256Hex) {
        return "POSSE;" + replica + ";" + pedido + ";" + sha256Hex;
    }

    public static String[] decode(String raw) {
        return raw.split(";", -1);
    }
}
