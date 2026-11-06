package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class Wire {
    private Wire() {}

    public static final String IPFS_API = "http://127.0.0.1:5001/api/v0";

    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    public static final long VERIFICACAO_MS = 200L;
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    public static final int GRUPO = 3;

    public static final int PORTO_BASE = 9000;

    // Limiar para o processador de alarmes
    public static final double LIMIAR_ALARME = Double.parseDouble(System.getProperty("raft04.limiar", "30.0"));

    public static String topicData(int k) {
        return "raft04-p" + k;
    }

    public static String topicIn(int k) {
        return "raft04-in-p" + k;
    }

    public static String topicElection(int k) {
        return "raft04-eleicao-p" + k;
    }

    public static int nodePort(int k, int i) {
        return PORTO_BASE + (k * 10) + i;
    }

    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    // Formatação da mensagem IN para envio de leituras
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }
}