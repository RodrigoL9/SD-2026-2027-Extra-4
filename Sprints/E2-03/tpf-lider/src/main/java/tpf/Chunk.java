package tpf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public final class Chunk {

    public static final class LogEntry {
        public final long lsn;
        public final long instante;
        public final String autor;
        public final String canal;
        public final String textoBase64;

        public LogEntry(long lsn, long instante, String autor, String canal, String textoBase64) {
            this.lsn = lsn;
            this.instante = instante;
            this.autor = autor;
            this.canal = canal;
            this.textoBase64 = textoBase64;
        }

        public String toLogLine() {
            return lsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoBase64;
        }

        public String decodedText() {
            return new String(Base64.getDecoder().decode(textoBase64), StandardCharsets.UTF_8);
        }

        public static LogEntry fromLogLine(String line) {
            String[] p = line.split(";", 5);
            return new LogEntry(
                    Long.parseLong(p[0]),
                    Long.parseLong(p[1]),
                    p[2],
                    p[3],
                    p[4]
            );
        }
    }

    private static final SecureRandom RNG = new SecureRandom();

    private Chunk() {}

    public static byte[] pack(List<LogEntry> entries, int padBytes) {
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : entries) {
            sb.append(e.toLogLine()).append('\n');
        }
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        int rawLen = 4 + payload.length;
        int blocks = Math.max(1, (rawLen + padBytes - 1) / padBytes);
        int totalLen = blocks * padBytes;

        byte[] padded = new byte[totalLen];
        if (totalLen > rawLen) {
            byte[] pad = new byte[totalLen - rawLen];
            RNG.nextBytes(pad);
            System.arraycopy(pad, 0, padded, rawLen, pad.length);
        }
        ByteBuffer.wrap(padded).putInt(payload.length);
        System.arraycopy(payload, 0, padded, 4, payload.length);
        return padded;
    }

    public static List<LogEntry> unpack(byte[] padded) {
        if (padded.length < 4) {
            throw new IllegalArgumentException("Chunk inválido: demasiado curto");
        }
        int payloadLen = ByteBuffer.wrap(padded).getInt();
        if (payloadLen < 0 || 4 + payloadLen > padded.length) {
            throw new IllegalArgumentException("Chunk inválido: tamanho de payload incorreto");
        }
        String text = new String(padded, 4, payloadLen, StandardCharsets.UTF_8);
        List<LogEntry> out = new ArrayList<>();
        for (String line : text.split("\n")) {
            if (!line.isBlank()) {
                out.add(LogEntry.fromLogLine(line));
            }
        }
        return out;
    }
}