package tpf;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class PubSubHub extends WebSocketServer {

    private static final class SessionState {
        String replicaId;
        PublicKey publicKey;
        byte[] challengeNonce;
        boolean authenticated = false;
        final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
    }

    private static final SecureRandom RNG = new SecureRandom();

    private final ReplicaRegistry registry;
    private final Consumer<String> leaderInbox;
    private final BiConsumer<String, String> eventLogger;
    private final Map<WebSocket, SessionState> sessions = new ConcurrentHashMap<>();

    public PubSubHub(int port, ReplicaRegistry registry, Consumer<String> leaderInbox,
                     BiConsumer<String, String> eventLogger) {
        super(new InetSocketAddress(port));
        setReuseAddr(true);
        this.registry = registry;
        this.leaderInbox = leaderInbox;
        this.eventLogger = eventLogger;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        sessions.put(conn, new SessionState());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        sessions.remove(conn);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        eventLogger.accept("ERRO", "WebSocket erro: " + ex.getMessage());
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
            if (!st.authenticated) {
                handleHandshake(conn, st, p);
                return;
            }

            if (p.length >= 2 && "SUB".equals(p[0])) {
                String topic = p[1];
                String expectedOwnTopic = Wire.topicReplica(st.replicaId);
                if (!topic.equals(expectedOwnTopic) && !topic.equals(Wire.TOPICO_CTL)) {
                    eventLogger.accept("ERRO", "Réplica " + st.replicaId + " tentou subscrever tópico proibido: " + topic);
                    conn.close(1008, "Tópico não autorizado");
                    return;
                }
                st.subscriptions.add(topic);
            } else if (p.length >= 3 && "PUB".equals(p[0])) {
                String topic = p[1];
                if (!Wire.TOPICO_LIDER.equals(topic)) {
                    eventLogger.accept("ERRO", "Réplica " + st.replicaId + " tentou publicar em tópico proibido: " + topic);
                    conn.close(1008, "Publicação não autorizada");
                    return;
                }
                int idx = message.indexOf(';', 4);
                String payload = message.substring(idx + 1);
                leaderInbox.accept(payload);
            } else {
                eventLogger.accept("ERRO", "Mensagem inválida da réplica " + st.replicaId + ": " + message);
                conn.close(1008, "Protocolo inválido");
            }
        } catch (Exception e) {
            eventLogger.accept("ERRO", "Falha no processamento WS: " + e.getMessage());
            conn.close(1008, e.getMessage());
        }
    }

    private void handleHandshake(WebSocket conn, SessionState st, String[] p) throws Exception {
        if ("OLA".equals(p[0]) && p.length >= 4 && st.challengeNonce == null) {
            String replicaId = p[1];
            String pubB64 = p[2];
            String token = p.length >= 5 ? p[4] : null;

            PublicKey pk = registry.authenticateOrRegister(replicaId, pubB64, token);
            byte[] nonce = new byte[32];
            RNG.nextBytes(nonce);

            st.replicaId = replicaId;
            st.publicKey = pk;
            st.challengeNonce = nonce;

            conn.send(Wire.encodeDesafio(Base64.getEncoder().encodeToString(nonce)));
        } else if ("PROVA".equals(p[0]) && p.length >= 3 && st.challengeNonce != null) {
            String replicaId = p[1];
            if (!replicaId.equals(st.replicaId)) {
                throw new SecurityException("ID inconsistente na PROVA");
            }
            byte[] sigBytes = Base64.getDecoder().decode(p[2]);
            Signature sig = Signature.getInstance("Ed25519");
            sig.initVerify(st.publicKey);
            sig.update(st.challengeNonce);
            if (!sig.verify(sigBytes)) {
                throw new SecurityException("Assinatura Ed25519 inválida para réplica " + replicaId);
            }
            st.authenticated = true;
            st.challengeNonce = null;
        } else {
            eventLogger.accept("ERRO", "Sequência de autenticação inválida antes de PROVA");
            conn.close(1008, "Autenticação obrigatória");
        }
    }

    public void publish(String topic, String mensagem) {
        String frame = Wire.encodePub(topic, mensagem);
        for (Map.Entry<WebSocket, SessionState> entry : sessions.entrySet()) {
            WebSocket ws = entry.getKey();
            SessionState st = entry.getValue();
            if (st.authenticated && st.subscriptions.contains(topic) && ws.isOpen()) {
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
