package tpf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ReplicaService implements WebSocket.Listener {

    private final String id;
    private final URI serverUri;
    private final long quotaBytes;
    private String registrationToken;

    private final Path dataDir;
    private final Path eventsFile;
    private final KeyPair keyPair;

    private final AtomicLong usedBytes = new AtomicLong(0L);
    private volatile WebSocket webSocket;
    private final StringBuilder rxBuffer = new StringBuilder();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private long backoffMs = Wire.BACKOFF_MIN_MS;

    public ReplicaService(String id, String url, long quotaBytes, String token) throws Exception {
        this.id = id;
        this.serverUri = URI.create(url);
        this.quotaBytes = quotaBytes;
        this.registrationToken = token;

        this.dataDir = Path.of("dados-" + id);
        Files.createDirectories(dataDir);
        this.eventsFile = Path.of("eventos-" + id + ".txt");

        this.keyPair = loadOrGenerateKeys(dataDir.resolve("chave.priv"), dataDir.resolve("chave.pub"));
        recalculateUsedBytes();
    }

    private KeyPair loadOrGenerateKeys(Path privFile, Path pubFile) throws Exception {
        KeyFactory kf = KeyFactory.getInstance("Ed25519");
        if (Files.exists(privFile) && Files.exists(pubFile)) {
            byte[] privBytes = Files.readAllBytes(privFile);
            byte[] pubBytes = Files.readAllBytes(pubFile);
            PrivateKey priv = kf.generatePrivate(new PKCS8EncodedKeySpec(privBytes));
            PublicKey pub = kf.generatePublic(new X509EncodedKeySpec(pubBytes));
            return new KeyPair(pub, priv);
        }
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("Ed25519");
        KeyPair kp = kpg.generateKeyPair();
        Files.write(privFile, kp.getPrivate().getEncoded());
        Files.write(pubFile, kp.getPublic().getEncoded());
        restrictOwnerOnly(privFile);
        return kp;
    }

    private static void restrictOwnerOnly(Path file) {
        try {
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException | IOException ignored) {
            File f = file.toFile();
            f.setReadable(false, false);
            f.setReadable(true, true);
            f.setWritable(false, false);
            f.setWritable(true, true);
        }
    }

    private void recalculateUsedBytes() throws IOException {
        long sum = 0L;
        try (var stream = Files.list(dataDir)) {
            for (Path p : stream.toList()) {
                if (p.getFileName().toString().endsWith(".bin")) {
                    sum += Files.size(p);
                }
            }
        }
        usedBytes.set(sum);
    }

    private synchronized void logEvent(String evento, String detalhe) {
        String line = System.currentTimeMillis() + ";" + evento + (detalhe == null || detalhe.isBlank() ? "" : ";" + detalhe) + "\n";
        try {
            Files.writeString(eventsFile, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public void start() {
        connect();
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, Wire.PERIODO_HB_MS, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private void connect() {
        reconnectScheduled.set(false);
        HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(serverUri, this)
                .whenComplete((ws, err) -> {
                    if (err != null) {
                        logEvent("ERRO", "Falha na ligação: " + err.getMessage());
                        scheduleReconnect();
                    } else {
                        this.webSocket = ws;
                        if (registrationToken != null && !registrationToken.isBlank()) {
                            String pubB64 = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());
                            ws.sendText(Wire.encodeOlaPrimeiro(id, pubB64, registrationToken), true);
                            this.registrationToken = null;
                        } else {
                            ws.sendText(Wire.encodeOla(id), true);
                        }
                    }
                });
    }

    private void scheduleReconnect() {
        if (!reconnectScheduled.compareAndSet(false, true)) return;
        long wait = backoffMs;
        backoffMs = Math.min(Wire.BACKOFF_MAX_MS, backoffMs * 2);
        logEvent("RELIGAR " + wait, "");
        scheduler.schedule(this::connect, wait, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        rxBuffer.append(data);
        if (last) {
            String msg = rxBuffer.toString();
            rxBuffer.setLength(0);
            handleFrame(webSocket, msg);
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        logEvent("DESLIGADO", "code=" + statusCode + " reason=" + reason);
        this.webSocket = null;
        scheduleReconnect();
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        logEvent("ERRO", error.getMessage());
        this.webSocket = null;
        scheduleReconnect();
    }

    private void handleFrame(WebSocket ws, String frame) {
        try {
            String[] p = Wire.decode(frame);
            if ("DESAFIO".equals(p[0]) && p.length >= 2) {
                String nonceB64 = p[1];
                byte[] payload = ("tpf-registo|" + id + "|" + nonceB64).getBytes(StandardCharsets.UTF_8);

                Signature sig = Signature.getInstance("Ed25519");
                sig.initSign(keyPair.getPrivate());
                sig.update(payload);
                String sigB64 = Base64.getEncoder().encodeToString(sig.sign());

                ws.sendText(Wire.encodeProva(id, sigB64), true);
                ws.sendText(Wire.encodeSub(Wire.topicReplica(id)), true);
                ws.sendText(Wire.encodeSub(Wire.TOPICO_CTL), true);

                this.backoffMs = Wire.BACKOFF_MIN_MS;
                logEvent("LIGADO", "");
                sendInventario();
                sendHeartbeat();
            } else if ("PUB".equals(p[0]) && p.length >= 3) {
                int idx = frame.indexOf(';', 4);
                String payload = frame.substring(idx + 1);
                handleProtocolMessage(payload);
            }
        } catch (Exception e) {
            logEvent("ERRO", "Erro ao processar trama: " + e.getMessage());
        }
    }

    private void handleProtocolMessage(String msg) throws Exception {
        String[] p = Wire.decode(msg);
        if ("STORE".equals(p[0]) && p.length >= 4) {
            handleStore(p[1], p[2], p[3]);
        }
    }

    private synchronized void handleStore(String chunkId, String hdrB64, String binB64) throws Exception {
        byte[] hdrBytes = Base64.getDecoder().decode(hdrB64);
        byte[] binBytes = Base64.getDecoder().decode(binB64);

        Path binPath = dataDir.resolve(chunkId + ".bin");
        Path hdrPath = dataDir.resolve(chunkId + ".hdr");

        if (Files.exists(binPath)) {
            String existingSha = sha256Hex(Files.readAllBytes(binPath));
            publishToLeader(Wire.encodeAck(id, chunkId, existingSha));
            return;
        }

        if (usedBytes.get() + binBytes.length > quotaBytes) {
            logEvent("QUOTA", "chunk=" + chunkId + " tamanho=" + binBytes.length);
            return;
        }

        String sha = sha256Hex(binBytes);
        if (!sha.equalsIgnoreCase(chunkId)) {
            logEvent("ERRO", "Hash inválido no STORE para " + chunkId);
            return;
        }

        Path tmpBin = dataDir.resolve(chunkId + ".bin.tmp");
        Files.write(tmpBin, binBytes);
        Files.move(tmpBin, binPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        Path tmpHdr = dataDir.resolve(chunkId + ".hdr.tmp");
        Files.write(tmpHdr, hdrBytes);
        Files.move(tmpHdr, hdrPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        recalculateUsedBytes();
        publishToLeader(Wire.encodeAck(id, chunkId, sha));
        logEvent("STORE", "chunk=" + chunkId + " bytes=" + binBytes.length);
    }

    private void sendHeartbeat() {
        WebSocket ws = this.webSocket;
        if (ws != null) {
            publishToLeader(Wire.encodeHb(id, usedBytes.get(), quotaBytes));
        }
    }

    private void sendInventario() {
        try {
            List<String> cids = listChunkIds();
            publishToLeader(Wire.encodeInventario(id, cids));
        } catch (Exception ignored) {}
    }

    private List<String> listChunkIds() throws IOException {
        List<String> out = new ArrayList<>();
        try (var stream = Files.list(dataDir)) {
            for (Path p : stream.toList()) {
                String name = p.getFileName().toString();
                if (name.endsWith(".bin")) {
                    out.add(name.substring(0, name.length() - 4));
                }
            }
        }
        return out;
    }

    private void publishToLeader(String msg) {
        WebSocket ws = this.webSocket;
        if (ws != null) {
            ws.sendText(Wire.encodePub(Wire.TOPICO_LIDER, msg), true);
        }
    }

    private static String sha256Hex(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: ReplicaService <id> <url> <quotaBytes> [token]");
            System.exit(1);
        }
        String id = args[0];
        String url = args[1];
        long quota = Long.parseLong(args[2]);
        String token = args.length >= 4 ? args[3] : null;

        ReplicaService svc = new ReplicaService(id, url, quota, token);
        svc.start();
        System.out.println("ReplicaService '" + id + "' iniciado (quota=" + quota + " bytes).");
        Thread.currentThread().join();
    }
}