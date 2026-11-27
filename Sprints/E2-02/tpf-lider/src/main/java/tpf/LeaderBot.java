package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.events.session.ReadyEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LeaderBot extends ListenerAdapter {

    private static final String ADMIN_ROLE = "admin-arquivo";
    private static final String ARCHIVE_CHANNEL = "arquivo";
    private static final Set<String> ADMIN_COMMANDS = Set.of(
            "config", "replica-token", "replica-remover", "rajada"
    );

    private final String guildId;
    private final boolean registerCommands;
    private final Path baseDir;
    private final Path logFile;
    private final Path sealedDir;
    private final Path eventsFile;

    private final ChunkCrypto crypto;
    private final Placement placement;
    private final ReplicaRegistry registry;
    private final Catalog catalog;
    private final PubSubHub hub;
    private final Reconciler reconciler;
    private final Recovery recovery;

    private final AtomicInteger currentR = new AtomicInteger(Wire.R);
    private final AtomicLong nextLsn = new AtomicLong(1L);

    private final Object openChunkLock = new Object();
    private final List<Chunk.LogEntry> openEntries = new ArrayList<>();
    private int openApproxBytes = 4;
    private ScheduledFuture<?> sealTimerFuture = null;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final Map<String, Integer> retryRounds = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> attemptedReplicas = new ConcurrentHashMap<>();

    public LeaderBot(String guildId, boolean registerCommands, Path baseDir) throws Exception {
        this.guildId = guildId;
        this.registerCommands = registerCommands;
        this.baseDir = baseDir;
        this.logFile = baseDir.resolve("registos.log");
        this.sealedDir = baseDir.resolve("selados");
        this.eventsFile = baseDir.resolve("eventos-lider.txt");
        Files.createDirectories(sealedDir);

        Path mkPath = baseDir.resolve("master.key");
        if (!Files.exists(mkPath)) {
            throw new IllegalStateException("Ficheiro master.key não encontrado em " + mkPath.toAbsolutePath() +
                    ". Gera-o com: head -c 32 /dev/urandom > master.key");
        }
        byte[] mk = Files.readAllBytes(mkPath);
        this.crypto = new ChunkCrypto(mk, 1);
        this.placement = new Placement(Wire.ALFA, Wire.MARGEM_BYTES);
        this.registry = new ReplicaRegistry(baseDir);
        this.catalog = new Catalog(baseDir);

        this.hub = new PubSubHub(Wire.PORTO, registry, this::onReplicaMessage, this::logEvent);
        this.recovery = new Recovery(logFile, registry, catalog, crypto, hub, this::logEvent);
        this.reconciler = new Reconciler(sealedDir, registry, catalog, placement, crypto, hub,
                currentR, this::logEvent, this::handleRepairFetchRequest);

        initLsnFromLog();
    }

    private void initLsnFromLog() throws IOException {
        if (!Files.exists(logFile)) return;
        long max = 0L;
        for (String line : Files.readAllLines(logFile)) {
            if (!line.isBlank()) {
                Chunk.LogEntry le = Chunk.LogEntry.fromLogLine(line);
                max = Math.max(max, le.lsn);
            }
        }
        nextLsn.set(max + 1);
    }

    public void startServices() throws Exception {
        hub.start();
        if (!catalog.existsOnDisk() && Files.exists(baseDir.resolve("replicas.txt")) && !registry.all().isEmpty()) {
            long recoveredMax = recovery.recuperarTudo(reconciler);
            if (recoveredMax > 0) {
                nextLsn.set(recoveredMax + 1);
            }
        }
        reconciler.start();
    }

    public static void main(String[] args) throws Exception {
        String token = loadConfig("DISCORD_TOKEN", "token.txt");
        String guildId = loadConfig("DISCORD_GUILD_ID", "guild.txt");

        if (token == null || token.isBlank()) {
            System.err.println("Erro: Token não encontrado. Define DISCORD_TOKEN ou cria token.txt.");
            System.exit(1);
        }
        if (guildId == null || guildId.isBlank()) {
            System.err.println("Erro: ID do servidor não encontrado. Define DISCORD_GUILD_ID ou cria guild.txt.");
            System.exit(1);
        }

        boolean registerCommands = Arrays.stream(args)
                .anyMatch(arg -> arg.equalsIgnoreCase("--register") || arg.equalsIgnoreCase("--register-commands"))
                || "true".equalsIgnoreCase(System.getenv("REGISTER_COMMANDS"));

        LeaderBot bot = new LeaderBot(guildId.trim(), registerCommands, Path.of("."));
        bot.startServices();

        JDA jda = JDABuilder.createDefault(token.trim())
                .enableIntents(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(bot)
                .build();

        jda.awaitReady();
    }

    private static String loadConfig(String envVar, String fallbackFile) {
        String val = System.getenv(envVar);
        if (val != null && !val.isBlank()) return val;
        try {
            Path path = Path.of(fallbackFile);
            if (Files.exists(path)) return Files.readString(path).trim();
        } catch (Exception ignored) {}
        return null;
    }

    private synchronized void logEvent(String evento, String detalhe) {
        String line = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.writeString(eventsFile, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Erro ao escrever evento: " + e.getMessage());
        }
    }

    // ==========================================================================
    // ESCRITA, LOG COM FSYNC, SELAGEM E COLOCAÇÃO
    // ==========================================================================
    public Chunk.LogEntry ingestRecord(String autor, String canal, String texto) throws Exception {
        long lsn = nextLsn.getAndIncrement();
        long now = System.currentTimeMillis();
        String b64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        Chunk.LogEntry entry = new Chunk.LogEntry(lsn, now, autor, canal, b64);

        byte[] lineBytes = (entry.toLogLine() + "\n").getBytes(StandardCharsets.UTF_8);
        synchronized (logFile) {
            try (FileChannel fc = FileChannel.open(logFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                fc.write(ByteBuffer.wrap(lineBytes));
                fc.force(true);
            }
        }
        logEvent("REGISTO", "lsn=" + lsn + " autor=" + autor + " canal=" + canal);

        boolean sealNow = false;
        synchronized (openChunkLock) {
            if (openEntries.isEmpty()) {
                sealTimerFuture = scheduler.schedule(this::sealOpenChunkSafe, Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
            }
            openEntries.add(entry);
            openApproxBytes += lineBytes.length;
            if (openApproxBytes >= Wire.CHUNK_MAX_BYTES) {
                sealNow = true;
            }
        }
        if (sealNow) {
            sealOpenChunkSafe();
        }
        return entry;
    }

    private void sealOpenChunkSafe() {
        List<Chunk.LogEntry> batch;
        synchronized (openChunkLock) {
            if (openEntries.isEmpty()) return;
            batch = new ArrayList<>(openEntries);
            openEntries.clear();
            openApproxBytes = 4;
            if (sealTimerFuture != null) {
                sealTimerFuture.cancel(false);
                sealTimerFuture = null;
            }
        }

        try {
            byte[] padded = Chunk.pack(batch, Wire.PAD_BYTES);
            ChunkCrypto.Sealed sealed = crypto.seal(padded);
            long lsnDe = batch.get(0).lsn;
            long lsnAte = batch.get(batch.size() - 1).lsn;

            String saltB64 = Base64.getEncoder().encodeToString(sealed.salt);
            String nonceB64 = Base64.getEncoder().encodeToString(sealed.nonce);
            String ctB64 = Base64.getEncoder().encodeToString(sealed.ciphertext);

            // Grava selados/<chunkId>.bin com envelope completo
            Files.writeString(sealedDir.resolve(sealed.id + ".bin"),
                    sealed.keyVersion + ";" + saltB64 + ";" + nonceB64 + ";" + ctB64);

            catalog.addSealedChunk(sealed.id, lsnDe, lsnAte, sealed.ciphertext);
            logEvent("SELADO", "chunk=" + sealed.id + " lsn=" + lsnDe + ".." + lsnAte + " bytes=" + sealed.ciphertext.length);

            retryRounds.put(sealed.id, 1);
            attemptedReplicas.put(sealed.id, ConcurrentHashMap.newKeySet());
            dispatchChunk(sealed.id, sealed.ciphertext.length);
        } catch (Exception e) {
            logEvent("ERRO", "Falha ao selar chunk: " + e.getMessage());
        }
    }

    private void dispatchChunk(String chunkId, long size) throws Exception {
        Catalog.Entry entry = catalog.get(chunkId);
        if (entry == null) return;

        int rTarget = currentR.get();
        int missing = rTarget - entry.replicas.size();
        if (missing <= 0) return;

        Set<String> exclude = new HashSet<>(attemptedReplicas.getOrDefault(chunkId, Set.of()));
        exclude.addAll(entry.replicas);

        List<String> chosen = registry.reserveAndChoose(chunkId, size, missing, exclude, placement);
        if (entry.replicas.size() + chosen.size() < rTarget) {
            logEvent("SUBREPLICADO", "chunk=" + chunkId + " escolhidas=" + chosen.size() + "/" + missing);
        }
        if (chosen.isEmpty()) return;

        String storeMsg = reconciler.buildStoreMessageFromSealed(chunkId);
        if (storeMsg == null) return;

        Set<String> tried = attemptedReplicas.computeIfAbsent(chunkId, k -> ConcurrentHashMap.newKeySet());
        for (String repId : chosen) {
            tried.add(repId);
            hub.publish(Wire.topicReplica(repId), storeMsg);
            logEvent("COLOCADO", "chunk=" + chunkId + " replica=" + repId);
        }

        scheduler.schedule(() -> onAckTimeout(chunkId, size, chosen), Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void onAckTimeout(String chunkId, long size, List<String> sentTargets) {
        for (String repId : sentTargets) {
            registry.releaseReservation(chunkId, repId, size);
        }
        Catalog.Entry entry = catalog.get(chunkId);
        if (entry == null) return;

        if (entry.replicas.size() < currentR.get()) {
            int round = retryRounds.getOrDefault(chunkId, 1);
            if (round < 3) {
                retryRounds.put(chunkId, round + 1);
                try {
                    dispatchChunk(chunkId, size);
                } catch (Exception e) {
                    logEvent("ERRO", "Erro no retry de colocação: " + e.getMessage());
                }
            }
        }
    }

    private void handleRepairFetchRequest(String reqId, String sourceReplica) {
        String[] parts = reqId.split(":", 4);
        String chunkId = parts[1];
        String targetReplica = parts[2];
        recovery.requestFetch(reqId, sourceReplica, chunkId).thenAccept(dp -> {
            String msg = Wire.encodeStore(dp.chunkId, dp.versaoChave,
                    Base64.getEncoder().encodeToString(dp.salt),
                    Base64.getEncoder().encodeToString(dp.nonce),
                    dp.ciphertext.length,
                    Base64.getEncoder().encodeToString(dp.ciphertext));
            hub.publish(Wire.topicReplica(targetReplica), msg);
        });
    }

    // ==========================================================================
    // MENSAGENS RECEBIDAS DAS RÉPLICAS NO TÓPICO tpf-lider
    // ==========================================================================
    private void onReplicaMessage(String raw) {
        try {
            String[] p = Wire.decode(raw);
            switch (p[0]) {
                case "HB" -> {
                    String repId = p[1];
                    long sentMs = Long.parseLong(p[2]);
                    long quota = Long.parseLong(p[3]);
                    long usados = Long.parseLong(p[4]);
                    int nChunks = Integer.parseInt(p[5]);
                    int verMan = Integer.parseInt(p[6]);
                    ReplicaRegistry.State prev = registry.updateHeartbeat(repId, sentMs, quota, usados, nChunks, verMan);
                    if (prev != null && prev != ReplicaRegistry.State.VIVA) {
                        logEvent("REGRESSO", repId + " voltou de " + prev + " para VIVA");
                    }
                }
                case "ACK" -> {
                    String repId = p[1];
                    String chunkId = p[2];
                    String sha = p[3];
                    Catalog.Entry entry = catalog.get(chunkId);
                    if (entry != null) {
                        registry.releaseReservation(chunkId, repId, entry.tamanho);
                        if (chunkId.equalsIgnoreCase(sha)) {
                            boolean durableNow = catalog.confirmReplica(chunkId, repId, Wire.W);
                            logEvent("ACK", "chunk=" + chunkId + " replica=" + repId);
                            if (durableNow) {
                                logEvent("DURAVEL", "chunk=" + chunkId + " lsn=" + entry.lsnDe + ".." + entry.lsnAte);
                            }
                        } else {
                            logEvent("INTEGRIDADE", "ACK com hash inválido de " + repId + " para " + chunkId);
                        }
                    }
                }
                case "DATA" -> recovery.onDataReceived(p[1], p[2], p[3], Integer.parseInt(p[4]), p[5], p[6], p[7]);
                case "INVENTARIO" -> {
                    String repId = p[1];
                    String pedido = p[2];
                    List<String> cids = (p.length < 5 || p[4].isBlank()) ? List.of() : Arrays.asList(p[4].split(","));
                    recovery.onInventarioReceived(repId, pedido, cids);
                    reconciler.handleInventario(repId, cids);
                }
                case "MANIFESTO" -> recovery.onManifestoReceived(p[1], Integer.parseInt(p[2]), p[3],
                        Integer.parseInt(p[4]), p[5], p[6], p[7]);
                case "POSSE" -> reconciler.handlePosseResponse(p[1], p[2], p[3]);
                default -> logEvent("ERRO", "Mensagem desconhecida em tpf-lider: " + raw);
            }
        } catch (Exception e) {
            logEvent("ERRO", "Erro ao tratar mensagem de réplica: " + e.getMessage());
        }
    }

    // ==========================================================================
    // DISCORD JDA
    // ==========================================================================
    @Override
    public void onReady(ReadyEvent event) {
        Guild guild = event.getJDA().getGuildById(guildId);
        if (guild == null) {
            System.err.println("Erro: Servidor com ID " + guildId + " não encontrado.");
            return;
        }
        if (!registerCommands) {
            System.out.println("LeaderBot online no servidor '" + guild.getName() + "' (usa --register para registar comandos).");
            return;
        }

        guild.updateCommands().addCommands(
                Commands.slash("registar", "Regista uma entrada no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),
                Commands.slash("estado", "Consulta o estado atual do líder e das réplicas"),
                Commands.slash("config", "Configura parâmetros do sistema")
                        .addSubcommands(
                                new SubcommandData("r", "Define o fator de replicação r")
                                        .addOption(OptionType.STRING, "n", "Número de réplicas (inteiro > 0)", true)
                        ),
                Commands.slash("recuperar", "Recupera registos num intervalo de sequência")
                        .addOption(OptionType.INTEGER, "de", "Número de sequência inicial (>= 1)", true)
                        .addOption(OptionType.INTEGER, "ate", "Número de sequência final (>= de)", true),
                Commands.slash("replica-token", "Gera um token de associação para uma nova réplica"),
                Commands.slash("replica-remover", "Remove uma réplica registada")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica", true),
                Commands.slash("rajada", "Envia uma rajada de registos de teste")
                        .addOption(OptionType.INTEGER, "n", "Número de mensagens a enviar (> 0)", true)
                        .addOption(OptionType.INTEGER, "intervaloms", "Intervalo em milissegundos (>= 0)", true)
        ).queue(
                cmds -> System.out.println("LeaderBot online! " + cmds.size() + " comandos registados."),
                err -> System.err.println("Erro ao registar comandos: " + err.getMessage())
        );
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String command = event.getName();
        if (ADMIN_COMMANDS.contains(command) && !hasAdminRole(event.getMember())) {
            event.reply("Permissão negada: apenas membros com o papel `" + ADMIN_ROLE + "` podem executar `/" + command + "`.")
                    .setEphemeral(true).queue();
            return;
        }

        switch (command) {
            case "registar" -> handleRegistar(event);
            case "estado" -> handleEstado(event);
            case "config" -> handleConfig(event);
            case "recuperar" -> handleRecuperar(event);
            case "replica-token" -> handleReplicaToken(event);
            case "replica-remover" -> handleReplicaRemover(event);
            case "rajada" -> handleRajada(event);
            default -> event.reply("Comando desconhecido").setEphemeral(true).queue();
        }
    }

    private void handleRegistar(SlashCommandInteractionEvent event) {
        OptionMapping opt = event.getOption("texto");
        String texto = opt != null ? opt.getAsString() : null;
        if (texto == null || texto.isBlank()) {
            event.reply("Erro: `texto` não pode estar vazio.").setEphemeral(true).queue();
            return;
        }
        try {
            Chunk.LogEntry entry = ingestRecord(event.getUser().getName(), event.getChannel().getName(), texto);
            event.reply("Registado com **LSN " + entry.lsn + "** (persistido em `registos.log`).").queue();
        } catch (Exception e) {
            event.reply("Erro ao registar: " + e.getMessage()).setEphemeral(true).queue();
        }
    }

    private void handleEstado(SlashCommandInteractionEvent event) {
        int rTarget = currentR.get();
        List<ReplicaRegistry.ReplicaInfo> reps = registry.all();
        List<Catalog.Entry> chunks = catalog.all();

        long subreplicados = chunks.stream().filter(c -> {
            long vivos = c.replicas.stream()
                    .map(registry::get)
                    .filter(r -> r != null && r.state != ReplicaRegistry.State.MORTA)
                    .count();
            return vivos < rTarget;
        }).count();

        StringBuilder sb = new StringBuilder();
        sb.append("**Estado do Líder** | R = `").append(rTarget).append("` | W = `").append(Wire.W)
          .append("` | Chunks = `").append(chunks.size()).append("` | Subreplicados = `").append(subreplicados).append("`\n");

        if (reps.isEmpty()) {
            sb.append("_Nenhuma réplica registada._");
        } else {
            for (ReplicaRegistry.ReplicaInfo r : reps) {
                double pct = r.quota > 0 ? (100.0 * r.usedBytes / r.quota) : 0.0;
                sb.append(String.format("• **%s** [%s] — %d / %d bytes (%.1f%%) | chunks: %d | manifesto v%d\n",
                        r.id, r.state, r.usedBytes, r.quota, pct, r.nChunks, r.manifestVersion));
            }
        }
        event.reply(sb.toString()).queue();
    }

    private void handleConfig(SlashCommandInteractionEvent event) {
        if (!"r".equals(event.getSubcommandName())) {
            event.reply("Subcomando inválido.").setEphemeral(true).queue();
            return;
        }
        OptionMapping opt = event.getOption("n");
        String rawN = opt != null ? opt.getAsString() : "";
        int n;
        try {
            n = Integer.parseInt(rawN.trim());
        } catch (NumberFormatException e) {
            event.reply("Erro: `n` deve ser um número inteiro (recebido: `" + rawN + "`).").setEphemeral(true).queue();
            return;
        }
        if (n <= 0) {
            event.reply("Erro: `n` deve ser > 0 (recebido: `" + n + "`).").setEphemeral(true).queue();
            return;
        }
        currentR.set(n);
        reconciler.triggerNow();
        event.reply("Fator de replicação atualizado para **R = " + n + "**. Reconciliação iniciada.").queue();
    }

    private void handleRecuperar(SlashCommandInteractionEvent event) {
        long de = event.getOption("de").getAsLong();
        long ate = event.getOption("ate").getAsLong();
        if (de < 1 || ate < de) {
            event.reply("Erro: intervalo inválido (`1 <= de <= ate`).").setEphemeral(true).queue();
            return;
        }
        event.deferReply().queue();
        scheduler.execute(() -> {
            try {
                List<Chunk.LogEntry> recovered = recovery.recuperar(de, ate);
                if (recovered.isEmpty()) {
                    event.getHook().sendMessage("Nenhum registo selado encontrado no intervalo `[" + de + ", " + ate + "]`.").queue();
                    return;
                }
                StringBuilder sb = new StringBuilder("**Registos recuperados (" + recovered.size() + "):**\n");
                for (Chunk.LogEntry le : recovered) {
                    String line = "`[" + le.lsn + "]` **" + le.autor + "** (#" + le.canal + "): " + le.decodedText() + "\n";
                    if (sb.length() + line.length() > 1900) {
                        sb.append("_... (truncado)_");
                        break;
                    }
                    sb.append(line);
                }
                event.getHook().sendMessage(sb.toString()).queue();
            } catch (Exception e) {
                event.getHook().sendMessage("Erro na recuperação: " + e.getMessage()).queue();
            }
        });
    }

    private void handleReplicaToken(SlashCommandInteractionEvent event) {
        String token = registry.generateToken();
        event.reply("Token de uso único gerado (válido por 10 minutos):\n`" + token + "`\n" +
                "Arranque da réplica:\n`java -cp target/classes tpf.ReplicaService <id> ws://localhost:" + Wire.PORTO + " <quotaBytes> " + token + "`")
                .setEphemeral(true).queue();
    }

    private void handleReplicaRemover(SlashCommandInteractionEvent event) {
        String id = event.getOption("id").getAsString().trim();
        if (id.isBlank()) {
            event.reply("Erro: `id` inválido.").setEphemeral(true).queue();
            return;
        }
        boolean removed = registry.removeReplica(id);
        if (removed) {
            catalog.removeReplicaEverywhere(id);
            hub.disconnectReplica(id);
            logEvent("REMOVIDO", "Réplica removida: " + id);
            reconciler.triggerNow();
            event.reply("Réplica **" + id + "** removida e desligada.").queue();
        } else {
            event.reply("Réplica **" + id + "** não encontrada.").setEphemeral(true).queue();
        }
    }

    private void handleRajada(SlashCommandInteractionEvent event) {
        long n = event.getOption("n").getAsLong();
        long intervaloMs = event.getOption("intervaloms").getAsLong();
        if (n <= 0 || intervaloMs < 0) {
            event.reply("Erro: `n > 0` e `intervaloms >= 0`.").setEphemeral(true).queue();
            return;
        }
        event.reply("Rajada iniciada: **" + n + "** registos com intervalo de **" + intervaloMs + " ms**.").queue();
        scheduler.execute(() -> {
            for (int i = 1; i <= n; i++) {
                try {
                    ingestRecord("rajada", ARCHIVE_CHANNEL, "registo-sintetico-" + i + "-" + UUID.randomUUID());
                    if (intervaloMs > 0 && i < n) {
                        Thread.sleep(intervaloMs);
                    }
                } catch (Exception e) {
                    logEvent("ERRO", "Erro na rajada: " + e.getMessage());
                    break;
                }
            }
        });
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot() || !event.isFromGuild()) return;
        if (!ARCHIVE_CHANNEL.equals(event.getChannel().getName())) return;

        try {
            Chunk.LogEntry entry = ingestRecord(
                    event.getAuthor().getName(),
                    event.getChannel().getName(),
                    event.getMessage().getContentDisplay()
            );
            event.getMessage().reply("Registado com **LSN " + entry.lsn + "**.").queue();
        } catch (Exception e) {
            logEvent("ERRO", "Erro ao registar mensagem: " + e.getMessage());
        }
    }

    private boolean hasAdminRole(Member member) {
        return member != null && member.getRoles().stream().anyMatch(r -> ADMIN_ROLE.equals(r.getName()));
    }
}
