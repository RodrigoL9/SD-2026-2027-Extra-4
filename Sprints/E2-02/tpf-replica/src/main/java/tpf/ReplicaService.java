package tpf;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.*;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class ReplicaService implements WebSocket.Listener {

    private final String id;
    private final URI serverUri;
    private final long quotaBytes;
    private String registrationToken;

    private final Path dataDir;
    private final Path eventsFile;
    private final Path manifestFile;
    private final KeyPair keyPair;

    private final AtomicLong usedBytes = new AtomicLong(0L);
    private volatile int manifestVersion = 0;
    private volatile WebSocket webSocket;
    private final StringBuilder rxBuffer = new StringBuilder();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private long backoffMs = 1000L;

    public ReplicaService(String id, String url, long quotaBytes, String token) throws Exception {
        this.id = id;
        this.serverUri = URI.create(url);
        this.quotaBytes = quotaBytes;
        this.registrationToken = token;

        this.dataDir = Path.of("dados-" + id);
        Files.createDirectories(dataDir);
        this.eventsFile = Path.of("eventos-" + id + ".txt");
        this.manifestFile = dataDir.resolve("manifesto.bin");

        this.keyPair = loadOrGenerateKeys(dataDir.resolve("chave.der"));
        recalculateUsedBytes();
        loadManifestVersion();
    }

    private KeyPair loadOrGenerateKeys(Path keyFile) throws Exception {
        if (Files.exists(keyFile)) {
            List<String> lines = Files.readAllLines(keyFile);
            byte[] privBytes = Base64.getDecoder().decode(lines.get(0));
            byte[] pubBytes = Base64.getDecoder().decode(lines.get(1));
            KeyFactory kf = KeyFactory.getInstance("Ed25519");
            PrivateKey priv = kf.generatePrivate(new PKCS8EncodedKeySpec(privBytes));
            PublicKey pub = kf.generatePublic(new X509EncodedKeySpec(pubBytes));
            return new KeyPair(pub, priv);
        }
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("Ed25519");
        KeyPair kp = kpg.generateKeyPair();
        String privB64 = Base64.getEncoder().encodeToString(kp.getPrivate().getEncoded());
        String pubB64 = Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());
        Files.write(keyFile, List.of(privB64, pubB64));
        return kp;
    }

    private void recalculateUsedBytes() throws IOException {
        long sum = 0L;
        try (var stream = Files.list(dataDir)) {
            for (Path p : stream.toList()) {
                if (p.getFileName().toString().endsWith(".bin") && !p.getFileName().toString().equals("manifesto.bin")) {
                    sum += Files.size(p);
                }
            }
        }
        usedBytes.set(sum);
    }

    private void loadManifestVersion() throws IOException {
        if (Files.exists(manifestFile)) {
            String raw = Files.readString(manifestFile);
            String[] p = Wire.decode(raw);
            if (p.length >= 2) {
                manifestVersion = Integer.parseInt(p[1]);
            }
        }
    }

    private synchronized void logEvent(String evento, String detalhe) {
        String line = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.writeString(eventsFile, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public void start() {
        connect();
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, Wire.PERIODO_HB_MS, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private void connect() {
        HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(serverUri, this)
                .whenComplete((ws, err) -> {
                    if (err != null) {
                        logEvent("ERRO", "Falha na ligação: " + err.getMessage());
                        scheduleReconnect();
                    } else {
                        this.webSocket = ws;
                        this.backoffMs = 1000L;
                        String pubB64 = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());
                        ws.sendText(Wire.encodeOla(id, pubB64, 1, registrationToken), true);
                        this.registrationToken = null; // token usado uma única vez
                    }
                });
    }

    private void scheduleReconnect() {
        long wait = backoffMs;
        backoffMs = Math.min(30000L, backoffMs * 2);
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
            if ("DESAFIO".equals(p[0])) {
                byte[] nonce = Base64.getDecoder().decode(p[1]);
                Signature sig = Signature.getInstance("Ed25519");
                sig.initSign(keyPair.getPrivate());
                sig.update(nonce);
                String sigB64 = Base64.getEncoder().encodeToString(sig.sign());

                ws.sendText(Wire.encodeProva(id, sigB64), true);
                ws.sendText(Wire.encodeSub(Wire.topicReplica(id)), true);
                ws.sendText(Wire.encodeSub(Wire.TOPICO_CTL), true);

                sendHeartbeat();
                sendInventario("boot-" + System.currentTimeMillis());
                logEvent("LIGADO", "Autenticado no hub");
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
        switch (p[0]) {
            case "STORE" -> handleStore(p[1], Integer.parseInt(p[2]), p[3], p[4], Integer.parseInt(p[5]), p[6]);
            case "FETCH" -> handleFetch(p[1], p[2]);
            case "DELETE" -> handleDelete(p[1]);
            case "INVENTARIO?" -> sendInventario(p[1]);
            case "MANIFESTO" -> handleManifesto(Integer.parseInt(p[1]), msg);
            case "MANIFESTO?" -> handleManifestoReq();
            case "POSSE?" -> handlePosseReq(p[1], p[2], Integer.parseInt(p[3]), Integer.parseInt(p[4]), p[5]);
        }
    }

    private synchronized void handleStore(String chunkId, int versaoChave, String saltB64,
                                          String nonceB64, int tamanho, String dadosB64) throws Exception {
        byte[] bytes = Base64.getDecoder().decode(dadosB64);
        Path binPath = dataDir.resolve(chunkId + ".bin");
        Path hdrPath = dataDir.resolve(chunkId + ".hdr");

        if (!Files.exists(binPath) && usedBytes.get() + bytes.length > quotaBytes) {
            logEvent("QUOTA", "Recusado chunk=" + chunkId + " tamanho=" + bytes.length);
            return;
        }

        String sha = sha256Hex(bytes);
        if (!sha.equalsIgnoreCase(chunkId)) {
            logEvent("INTEGRIDADE", "Hash inválido no STORE para " + chunkId);
            return;
        }

        // Gravação atómica ANTES de enviar ACK
        Path tmpBin = dataDir.resolve(chunkId + ".bin.tmp");
        Files.write(tmpBin, bytes);
        Files.move(tmpBin, binPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        Path tmpHdr = dataDir.resolve(chunkId + ".hdr.tmp");
        Files.writeString(tmpHdr, chunkId + ";" + versaoChave + ";" + saltB64 + ";" + nonceB64 + ";" + bytes.length);
        Files.move(tmpHdr, hdrPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        recalculateUsedBytes();
        publishToLeader(Wire.encodeAck(id, chunkId, sha));
        logEvent("STORE", "chunk=" + chunkId + " bytes=" + bytes.length);
    }

    private synchronized void handleFetch(String pedido, String chunkId) throws Exception {
        Path binPath = dataDir.resolve(chunkId + ".bin");
        Path hdrPath = dataDir.resolve(chunkId + ".hdr");
        if (!Files.exists(binPath) || !Files.exists(hdrPath)) return;

        byte[] bytes = Files.readAllBytes(binPath);
        String sha = sha256Hex(bytes);
        if (!sha.equalsIgnoreCase(chunkId)) {
            Files.deleteIfExists(binPath);
            Files.deleteIfExists(hdrPath);
            recalculateUsedBytes();
            logEvent("INTEGRIDADE", "Corrupção detetada no FETCH chunk=" + chunkId + "; apagado");
            return;
        }

        String[] hdr = Wire.decode(Files.readString(hdrPath));
        String dadosB64 = Base64.getEncoder().encodeToString(bytes);
        publishToLeader(Wire.encodeData(id, pedido, chunkId, Integer.parseInt(hdr[1]), hdr[2], hdr[3], dadosB64));
    }

    private synchronized void handleDelete(String chunkId) throws IOException {
        Files.deleteIfExists(dataDir.resolve(chunkId + ".bin"));
        Files.deleteIfExists(dataDir.resolve(chunkId + ".hdr"));
        recalculateUsedBytes();
        logEvent("DELETE", "chunk=" + chunkId);
    }

    private synchronized void handleManifesto(int versao, String fullMsg) throws IOException {
        if (versao > manifestVersion) {
            Path tmp = dataDir.resolve("manifesto.bin.tmp");
            Files.writeString(tmp, fullMsg);
            Files.move(tmp, manifestFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            manifestVersion = versao;
            logEvent("MANIFESTO", "Guardado manifesto v" + versao);
        }
    }

    private synchronized void handleManifestoReq() throws IOException {
        if (Files.exists(manifestFile)) {
            publishToLeader(Files.readString(manifestFile));
        }
    }

    private synchronized void handlePosseReq(String pedido, String chunkId, int inicio, int comprimento, String nonceB64) throws Exception {
        Path binPath = dataDir.resolve(chunkId + ".bin");
        if (!Files.exists(binPath)) return;
        byte[] bytes = Files.readAllBytes(binPath);
        if (inicio < 0 || inicio + comprimento > bytes.length) return;

        byte[] nonce = Base64.getDecoder().decode(nonceB64);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(nonce);
        md.update(bytes, inicio, comprimento);
        String hashHex = HexFormat.of().formatHex(md.digest());

        publishToLeader(Wire.encodePosse(id, pedido, hashHex));
    }

    private void sendHeartbeat() {
        try {
            int nChunks = listChunkIds().size();
            publishToLeader(Wire.encodeHb(id, System.currentTimeMillis(), quotaBytes, usedBytes.get(), nChunks, manifestVersion));
        } catch (Exception ignored) {}
    }

    private void sendInventario(String pedido) {
        try {
            List<String> cids = listChunkIds();
            publishToLeader(Wire.encodeInventario(id, pedido, cids));
        } catch (Exception ignored) {}
    }

    private List<String> listChunkIds() throws IOException {
        List<String> out = new ArrayList<>();
        try (var stream = Files.list(dataDir)) {
            for (Path p : stream.toList()) {
                String name = p.getFileName().toString();
                if (name.endsWith(".bin") && !name.equals("manifesto.bin")) {
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
