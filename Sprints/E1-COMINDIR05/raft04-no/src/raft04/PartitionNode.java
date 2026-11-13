package raft04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class PartitionNode {

    private final int k;
    private final int i;
    private final int N;

    private final Path noFile;
    private final Path estadoFile;
    private final Path hbFile;
    private final Path eventosFile;

    private final IpfsPubSub ipfs;
    private final RaftState raftState;
    private final Object nodeLock = new Object();

    // Estado de dados em memória
    private final List<SensorRecord> records = new ArrayList<>();
    private final List<SensorRecord> pendentes = new ArrayList<>();
    private long seq = 0;
    private long lamport = 0;
    private VectorClock vector;
    private long hb = 0;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> batchFuture;
    private ServerSocket tcpServer;

    public PartitionNode(int k, int i, int N) throws IOException {
        this.k = k;
        this.i = i;
        this.N = N;

        this.noFile = Paths.get("no-p" + k + "-n" + i + ".txt");
        this.estadoFile = Paths.get("estado-p" + k + "-n" + i + ".txt");
        this.hbFile = Paths.get("hb-p" + k + "-n" + i + ".txt");
        this.eventosFile = Paths.get("eventos-p" + k + "-n" + i + ".txt");

        // 1. Carregar registos existentes do ficheiro local
        loadRecords();

        // 2. Inicializar RaftState
        this.raftState = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, estadoFile, this::logEvent);

        // 3. Inicializar IpfsPubSub com o URL HTTP correto da API
        this.ipfs = new IpfsPubSub(Wire.IPFS_API);
    }

    private VectorClock copyVector(VectorClock vc) {
        if (vc == null) return new VectorClock(N);
        return VectorClock.parse(vc.toString(), N);
    }

    private void loadRecords() throws IOException {
        synchronized (nodeLock) {
            records.clear();
            if (Files.exists(noFile)) {
                List<String> lines = Files.readAllLines(noFile, StandardCharsets.UTF_8);
                for (String line : lines) {
                    if (!line.isBlank()) {
                        SensorRecord r = SensorRecord.fromLine(line);
                        records.add(r);
                    }
                }
            }
            if (!records.isEmpty()) {
                SensorRecord last = records.get(records.size() - 1);
                this.seq = last.getSeq();
                this.lamport = last.getLamport();
                this.vector = last.getVector() != null ? copyVector(last.getVector()) : new VectorClock(N);
            } else {
                this.seq = 0;
                this.lamport = 0;
                this.vector = new VectorClock(N);
            }
        }
    }

    private long myLastSeq() {
        return records.isEmpty() ? 0 : records.get(records.size() - 1).getSeq();
    }

    private long myLastTerm() {
        return records.isEmpty() ? 0 : records.get(records.size() - 1).getTerm();
    }

    private SensorRecord getRecordBySeq(long s) {
        for (SensorRecord r : records) {
            if (r.getSeq() == s) return r;
        }
        return null;
    }

    private void logEvent(String msg) {
        long now = System.currentTimeMillis();
        String line = now + ";" + msg;
        System.out.println(line);
        try {
            Files.writeString(eventosFile, line + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public void start() throws Exception {
        logEvent("ARRANQUE;p" + k + ";no=n" + i + ";porta=" + Wire.nodePort(k, i));

        startTcpServer();

        // Agendar verificação periódica de timeouts de eleição (tick)
        scheduler.scheduleAtFixedRate(this::checkTick, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        // Subscrições PubSub executadas em segundo plano (evita bloquear a thread principal)
        startPubSub();
    }

    private void startTcpServer() {
        new Thread(() -> {
            try {
                tcpServer = new ServerSocket(Wire.nodePort(k, i));
                while (!tcpServer.isClosed()) {
                    Socket sock = tcpServer.accept();
                    new Thread(() -> handleTcpClient(sock)).start();
                }
            } catch (IOException e) {
                if (!tcpServer.isClosed()) {
                    logEvent("ERRO;p" + k + ";tcp_server=" + e.getMessage());
                }
            }
        }, "TCP-Recovery-Server").start();
    }

    private void handleTcpClient(Socket sock) {
        try (sock;
             BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8), true)) {

            if (Wire.isolated(k, i)) return;

            String line = reader.readLine();
            if (line == null || !line.startsWith("PEDIDO;")) return;

            String[] parts = line.split(";");
            long fromSeq = Long.parseLong(parts[parts.length - 1]);

            List<SensorRecord> toSend = new ArrayList<>();
            synchronized (nodeLock) {
                for (SensorRecord r : records) {
                    if (r.getSeq() >= fromSeq) {
                        toSend.add(r);
                    }
                }
            }
            for (SensorRecord r : toSend) {
                writer.println(r.toLine());
            }
            writer.println("FIM");
        } catch (Exception ignored) {}
    }

    private void startPubSub() {
        new Thread(() -> {
            try {
                ipfs.subscribe(Wire.topicIn(k), this::onInMessage);
            } catch (Exception e) {
                logEvent("ERRO;p" + k + ";sub_in=" + e.getMessage());
            }
        }, "Sub-In-p" + k).start();

        new Thread(() -> {
            try {
                ipfs.subscribe(Wire.topicElection(k), this::onElectionMessage);
            } catch (Exception e) {
                logEvent("ERRO;p" + k + ";sub_eleicao=" + e.getMessage());
            }
        }, "Sub-Election-p" + k).start();

        new Thread(() -> {
            try {
                ipfs.subscribe(Wire.topicData(k), this::onDataMessage);
            } catch (Exception e) {
                logEvent("ERRO;p" + k + ";sub_data=" + e.getMessage());
            }
        }, "Sub-Data-p" + k).start();
    }

    private void checkTick() {
        try {
            if (Wire.isolated(k, i)) return;

            long candidateTerm = raftState.tick();
            if (candidateTerm > 0) {
                long lastSeq, lastTerm;
                synchronized (nodeLock) {
                    lastSeq = myLastSeq();
                    lastTerm = myLastTerm();
                }
                VoteRequest req = new VoteRequest(candidateTerm, i, lastSeq, lastTerm);
                try {
                    ipfs.publish(Wire.topicElection(k), req.encode());
                } catch (Exception e) {
                    logEvent("ERRO;p" + k + ";pub_pedido_voto=" + e.getMessage());
                }
            }
        } catch (Throwable t) {
            logEvent("ERRO;p" + k + ";tick=" + t.getMessage());
        }
    }

    private void onElectionMessage(String msg) {
        if (Wire.isolated(k, i)) return;

        if (msg.startsWith("PEDIDO_VOTO;")) {
            try {
                VoteRequest req = VoteRequest.decode(msg);
                if (req.getCandidate() == i) return;

                long lastSeq, lastTerm;
                synchronized (nodeLock) {
                    lastSeq = myLastSeq();
                    lastTerm = myLastTerm();
                }

                RaftState.Vote vote = raftState.onVoteRequest(req.getTerm(), req.getCandidate(),
                        req.getLastSeq(), req.getLastTerm(), lastSeq, lastTerm);

                VoteReply reply = new VoteReply(vote.term, i, req.getCandidate(), vote.granted);
                try {
                    ipfs.publish(Wire.topicElection(k), reply.encode());
                } catch (Exception e) {
                    logEvent("ERRO;p" + k + ";pub_resposta_voto=" + e.getMessage());
                }
            } catch (Exception e) {
                logEvent("ERRO;p" + k + ";pedido_voto=" + e.getMessage());
            }
        } else if (msg.startsWith("VOTO;")) {
            try {
                VoteReply rep = VoteReply.decode(msg);
                if (rep.getCandidate() != i) return;

                boolean becameLeader = raftState.onVote(rep.getTerm(), rep.getVoter(), rep.getCandidate(), rep.isGranted());
                if (becameLeader) {
                    onBecomeLeader();
                }
            } catch (Exception e) {
                logEvent("ERRO;p" + k + ";voto=" + e.getMessage());
            }
        }
    }

    private void onBecomeLeader() {
        synchronized (nodeLock) {
            seq = myLastSeq();
            if (!records.isEmpty()) {
                SensorRecord last = records.get(records.size() - 1);
                lamport = last.getLamport();
                vector = last.getVector() != null ? copyVector(last.getVector()) : new VectorClock(N);
            } else {
                lamport = 0;
                vector = new VectorClock(N);
            }
            hb = 0;
            pendentes.clear();

            if (batchFuture != null && !batchFuture.isDone()) {
                batchFuture.cancel(true);
            }
            batchFuture = scheduler.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void onInMessage(String msg) {
        if (!msg.startsWith("IN;")) return;
        String[] parts = msg.split(";");
        if (parts.length < 5) return;

        String sensor = parts[1];
        double temp = Double.parseDouble(parts[2]);
        long inL = Long.parseLong(parts[3]);
        VectorClock inV = VectorClock.parse(parts[4], N);

        synchronized (nodeLock) {
            long t = raftState.termIfLeader();
            if (t == -1) {
                logEvent("IGNORADO;p" + k + ";leitura=" + sensor + " " + temp);
                return;
            }

            seq++;
            lamport = Math.max(lamport, inL) + 1;
            vector.merge(inV);
            vector.tick(k);

            SensorRecord r = SensorRecord.now(seq, t, sensor, temp, lamport, copyVector(vector));
            records.add(r);
            appendRecordFile(r);
            pendentes.add(r);
        }
    }

    private void enviarLote() {
        List<SensorRecord> aEnviar;
        long t;
        long currentHb;
        long lastSeq;
        long lastTerm;

        synchronized (nodeLock) {
            t = raftState.termIfLeader();
            if (t == -1) {
                if (batchFuture != null) batchFuture.cancel(false);
                return;
            }
            aEnviar = new ArrayList<>(pendentes);
            pendentes.clear();
            hb++;
            currentHb = hb;
            lastSeq = myLastSeq();
            lastTerm = myLastTerm();
        }

        Batch b = new Batch(t, i, "127.0.0.1", Wire.nodePort(k, i), currentHb, lastSeq, lastTerm, aEnviar);
        String encoded = b.encode();

        if (!Wire.isolated(k, i)) {
            try {
                ipfs.publish(Wire.topicData(k), encoded);
            } catch (Exception e) {
                logEvent("ERRO;p" + k + ";publish=" + e.getMessage());
            }
        }

        byte[] bytes = encoded.getBytes(StandardCharsets.UTF_8);
        String line = t + ";" + currentHb + ";" + System.currentTimeMillis() + ";"
                + lastSeq + ";" + lastTerm + ";" + aEnviar.size() + ";" + bytes.length;
        try {
            Files.writeString(hbFile, line + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    private void onDataMessage(String msg) {
        if (Wire.isolated(k, i) || !msg.startsWith("LOTE;")) return;

        Batch b;
        try {
            b = Batch.decode(msg);
        } catch (Exception e) {
            return;
        }

        if (b.getLeader() == i) return;

        boolean aceitar = raftState.onBatch(b.getTerm(), b.getLeader());
        if (!aceitar) {
            logEvent("IGNORADO;p" + k + ";termo=" + b.getTerm());
            return;
        }

        logEvent("RECEBIDO;p" + k + ";hb=" + b.getHb() + ";seq=" + b.getLastSeq());

        synchronized (nodeLock) {
            if (batchFuture != null && !batchFuture.isDone()) {
                batchFuture.cancel(false);
            }

            boolean diverge = false;
            for (SensorRecord r : b.getRecords()) {
                SensorRecord local = getRecordBySeq(r.getSeq());
                if (local != null) {
                    if (local.getTerm() != r.getTerm()) {
                        diverge = true;
                        break;
                    }
                } else {
                    long currentLast = myLastSeq();
                    if (r.getSeq() == currentLast + 1) {
                        records.add(r);
                        appendRecordFile(r);
                        lamport = Math.max(lamport, r.getLamport());
                        vector.merge(r.getVector());
                    } else if (r.getSeq() > currentLast + 1) {
                        break;
                    }
                }
            }

            long localLastSeq = myLastSeq();
            long localLastTerm = myLastTerm();

            if (diverge || localLastSeq > b.getLastSeq() || (localLastSeq == b.getLastSeq() && localLastSeq > 0 && localLastTerm != b.getLastTerm())) {
                resolveDivergence(b.getLeader(), b.getAddress(), b.getPort(), b.getLastSeq(), b.getLastTerm());
            } else if (localLastSeq < b.getLastSeq()) {
                recoverGaps(b.getLeader(), b.getAddress(), b.getPort(), localLastSeq + 1);
            }
        }
    }

    private void recoverGaps(int leaderId, String host, int port, long fromSeq) {
        if (Wire.isolated(k, i)) return;
        List<SensorRecord> fetched = fetchRecordsTcp(host, port, fromSeq);
        if (fetched.isEmpty()) return;

        for (SensorRecord r : fetched) {
            if (r.getSeq() == myLastSeq() + 1) {
                records.add(r);
                appendRecordFile(r);
                lamport = Math.max(lamport, r.getLamport());
                vector.merge(r.getVector());
            }
        }
        logEvent("RECUPERACAO;p" + k + ";de=" + fromSeq + ";ate=" + myLastSeq());
    }

    private void resolveDivergence(int leaderId, String host, int port, long leaderLastSeq, long leaderLastTerm) {
        if (Wire.isolated(k, i)) return;

        int idx = records.size() - 1;
        long startBlockSeq = 1;

        while (idx >= 0) {
            long currentBlockTerm = records.get(idx).getTerm();
            while (idx >= 0 && records.get(idx).getTerm() == currentBlockTerm) {
                startBlockSeq = records.get(idx).getSeq();
                idx--;
            }

            List<SensorRecord> fetched = fetchRecordsTcp(host, port, startBlockSeq);
            if (!fetched.isEmpty() && fetched.get(0).getSeq() == startBlockSeq) {
                SensorRecord localStart = getRecordBySeq(startBlockSeq);
                if (localStart != null && localStart.getTerm() == fetched.get(0).getTerm()) {
                    break;
                }
            }
            if (idx < 0) {
                startBlockSeq = 1;
            }
        }

        List<SensorRecord> fetchedFinal = fetchRecordsTcp(host, port, startBlockSeq);
        if (fetchedFinal.isEmpty() && startBlockSeq > 1) return;

        long divSeq = startBlockSeq;
        for (SensorRecord fr : fetchedFinal) {
            SensorRecord lr = getRecordBySeq(fr.getSeq());
            if (lr == null || lr.getTerm() != fr.getTerm()) {
                divSeq = fr.getSeq();
                break;
            }
        }

        List<SensorRecord> mantidos = new ArrayList<>();
        Set<Long> descartadosTermos = new LinkedHashSet<>();
        int descartados = 0;

        for (SensorRecord r : records) {
            if (r.getSeq() < divSeq) {
                mantidos.add(r);
            } else {
                descartados++;
                descartadosTermos.add(r.getTerm());
            }
        }

        for (SensorRecord r : fetchedFinal) {
            if (r.getSeq() >= divSeq) {
                mantidos.add(r);
            }
        }

        records.clear();
        records.addAll(mantidos);
        rewriteFileAtomic();

        if (!records.isEmpty()) {
            SensorRecord last = records.get(records.size() - 1);
            lamport = last.getLamport();
            vector = last.getVector() != null ? copyVector(last.getVector()) : new VectorClock(N);
        }

        logEvent("TRUNCADO;p" + k + ";de=" + divSeq + ";descartados=" + descartados + ";termos=" + descartadosTermos);
    }

    private List<SensorRecord> fetchRecordsTcp(String host, int port, long fromSeq) {
        List<SensorRecord> list = new ArrayList<>();
        try (Socket s = new Socket(host, port);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {

            out.println("PEDIDO;" + fromSeq);
            String line;
            while ((line = in.readLine()) != null) {
                if ("FIM".equals(line.trim())) break;
                if (!line.isBlank()) {
                    list.add(SensorRecord.fromLine(line));
                }
            }
        } catch (Exception ignored) {}
        return list;
    }

    private void appendRecordFile(SensorRecord r) {
        try {
            Files.writeString(noFile, r.toLine() + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    private void rewriteFileAtomic() {
        try {
            Path tmp = noFile.resolveSibling(noFile.getFileName() + ".tmp");
            StringBuilder sb = new StringBuilder();
            for (SensorRecord r : records) {
                sb.append(r.toLine()).append("\n");
            }
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
            Files.move(tmp, noFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java raft04.PartitionNode <k> <i> [N]");
            System.exit(1);
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int N = args.length >= 3 ? Integer.parseInt(args[2]) : 3;

        PartitionNode node = new PartitionNode(k, i, N);
        node.start();
    }
}