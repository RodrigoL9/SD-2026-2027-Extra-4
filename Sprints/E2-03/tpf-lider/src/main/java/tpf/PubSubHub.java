package tpf;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public final class PubSubHub extends WebSocketServer {

    private enum ConnState {
        NOVA, DESAFIADA, AUTENTICADA
    }

    private static final class SessionState {
        ConnState state = ConnState.NOVA;
        String replicaId;
        PublicKey publicKey;
        String publicKeyB64;
        String pendingToken;
        boolean firstRegistration;
        String challengeNonceB64;
    }

    private static final SecureRandom RNG = new SecureRandom();

    private final ReplicaRegistry registry;
    private final BiConsumer<String, String> leaderInbox;
    private final BiConsumer<String, String> eventLogger;
    private final Map<WebSocket, SessionState> sessions = new ConcurrentHashMap<>();
    private final Map<String, Set<WebSocket>> subscricoes = new ConcurrentHashMap<>();
    private final Map<String, Long> tokens = new ConcurrentHashMap<>();

    public PubSubHub(int port, ReplicaRegistry registry, BiConsumer<String, String> leaderInbox,
                     BiConsumer<String, String> eventLogger) {
        super(new InetSocketAddress(port));
        setReuseAddr(true);
        this.registry = registry;
        this.leaderInbox = leaderInbox;
        this.eventLogger = eventLogger;
    }

    public String gerarToken() {
        long now = System.currentTimeMillis();
        tokens.entrySet().removeIf(e -> now - e.getValue() > Wire.TOKEN_VALIDADE_MS);
        byte[] raw = new byte[18];
        RNG.nextBytes(raw);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        tokens.put(token, now);
        return token;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        sessions.put(conn, new SessionState());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        sessions.remove(conn);
        for (Set<WebSocket> subs : subscricoes.values()) {
            subs.remove(conn);
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        eventLogger.accept("ERRO", "WebSocket erro: " + (ex != null ? ex.getMessage() : "desconhecido"));
    }

    @Override
    public void onStart() {
        System.out.println("PubSubHub WebSocket ativo no porto " + getPort());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        SessionState st = sessions.get(conn);
        if (st == null) return;

        try {
            String[] p = Wire.decode(message);

            if (st.state == ConnState.NOVA) {
                if (!"OLA".equals(p[0])) {
                    throw new SecurityException("Mensagem fora de ordem antes de OLA: " + p[0]);
                }
                handleOla(conn, st, p);
            } else if (st.state == ConnState.DESAFIADA) {
                if (!"PROVA".equals(p[0]) || p.length != 3) {
                    throw new SecurityException("Esperado PROVA após DESAFIO, recebido: " + p[0]);
                }
                verificarProva(st, p[1], p[2]);
            } else {
                if ("SUB".equals(p[0]) && p.length == 2) {
                    String topico = p[1];
                    String proprio = Wire.topicReplica(st.replicaId);
                    if (!topico.equals(proprio) && !topico.equals(Wire.TOPICO_CTL)) {
                        throw new SecurityException("Subscrição não autorizada de " + st.replicaId + " em " + topico);
                    }
                    subscricoes.computeIfAbsent(topico, k -> ConcurrentHashMap.newKeySet()).add(conn);
                } else if ("PUB".equals(p[0]) && p.length >= 3) {
                    String topico = p[1];
                    if (!Wire.TOPICO_LIDER.equals(topico)) {
                        throw new SecurityException("Publicação não autorizada de " + st.replicaId + " em " + topico);
                    }
                    int idx = message.indexOf(';', 4);
                    String payload = message.substring(idx + 1);
                    leaderInbox.accept(st.replicaId, payload);
                } else {
                    throw new SecurityException("Comando inválido em estado AUTENTICADA: " + p[0]);
                }
            }
        } catch (Exception e) {
            eventLogger.accept("ERRO", e.getMessage());
            conn.close(1008, e.getMessage());
        }
    }

    private void handleOla(WebSocket conn, SessionState st, String[] p) throws Exception {
        long now = System.currentTimeMillis();
        tokens.entrySet().removeIf(e -> now - e.getValue() > Wire.TOKEN_VALIDADE_MS);

        if (p.length == 4) {
            String id = p[1];
            String pubB64 = p[2];
            String token = p[3];

            if (id.isBlank() || pubB64.isBlank() || token.isBlank()) {
                throw new SecurityException("Campos inválidos no 1.º OLA");
            }
            if (registry.isRegistered(id)) {
                throw new SecurityException("ID já registado recusado no 1.º registo: " + id);
            }
            Long created = tokens.get(token);
            if (created == null || now - created > Wire.TOKEN_VALIDADE_MS) {
                tokens.remove(token);
                throw new SecurityException("Token inválido, expirado ou já utilizado");
            }

            PublicKey pk = ReplicaRegistry.decodePublicKey(pubB64);
            byte[] nonce = new byte[32];
            RNG.nextBytes(nonce);
            String nonceB64 = Base64.getEncoder().encodeToString(nonce);

            st.replicaId = id;
            st.publicKey = pk;
            st.publicKeyB64 = pubB64;
            st.pendingToken = token;
            st.firstRegistration = true;
            st.challengeNonceB64 = nonceB64;
            st.state = ConnState.DESAFIADA;

            conn.send(Wire.encodeDesafio(nonceB64));
        } else if (p.length == 2) {
            String id = p[1];
            PublicKey pk = registry.getPublicKey(id);
            if (pk == null) {
                throw new SecurityException("Réplica desconhecida em OLA: " + id);
            }

            byte[] nonce = new byte[32];
            RNG.nextBytes(nonce);
            String nonceB64 = Base64.getEncoder().encodeToString(nonce);

            st.replicaId = id;
            st.publicKey = pk;
            st.firstRegistration = false;
            st.challengeNonceB64 = nonceB64;
            st.state = ConnState.DESAFIADA;

            conn.send(Wire.encodeDesafio(nonceB64));
        } else {
            throw new SecurityException("Formato de OLA inválido");
        }
    }

    public void verificarProva(SessionState st, String replicaId, String assinaturaB64) throws Exception {
        if (!st.replicaId.equals(replicaId)) {
            throw new SecurityException("ID inconsistente na PROVA: " + replicaId);
        }
        String nonceB64 = st.challengeNonceB64;
        st.challengeNonceB64 = null;
        if (nonceB64 == null) {
            throw new SecurityException("PROVA sem desafio ativo");
        }

        byte[] payload = ("tpf-registo|" + replicaId + "|" + nonceB64).getBytes(StandardCharsets.UTF_8);
        byte[] sigBytes = Base64.getDecoder().decode(assinaturaB64);

        Signature sig = Signature.getInstance("Ed25519");
        sig.initVerify(st.publicKey);
        sig.update(payload);
        if (!sig.verify(sigBytes)) {
            throw new SecurityException("Assinatura Ed25519 inválida para " + replicaId);
        }

        if (st.firstRegistration) {
            Long created = tokens.remove(st.pendingToken);
            if (created == null || System.currentTimeMillis() - created > Wire.TOKEN_VALIDADE_MS) {
                throw new SecurityException("Token expirado ou consumido concorrentemente");
            }
            registry.registerReplica(st.replicaId, st.publicKeyB64, st.publicKey);
        }

        st.state = ConnState.AUTENTICADA;
    }

    public void publish(String topico, String mensagem) {
        String frame = Wire.encodePub(topico, mensagem);
        Set<WebSocket> subs = subscricoes.get(topico);
        if (subs == null) return;
        for (WebSocket ws : subs) {
            if (ws.isOpen()) {
                ws.send(frame);
            }
        }
    }

    public void disconnectReplica(String replicaId) {
        for (Map.Entry<WebSocket, SessionState> entry : sessions.entrySet()) {
            if (replicaId.equals(entry.getValue().replicaId)) {
                entry.getKey().close(1000, "Réplica removida pelo administrador");
            }
        }
    }
}