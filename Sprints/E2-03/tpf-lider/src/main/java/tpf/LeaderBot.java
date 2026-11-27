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

    private final AtomicInteger currentR = new AtomicInteger(Wire.R);
    private final AtomicLong nextLsn = new AtomicLong(1L);

    private final Object openChunkLock = new Object();
    private final List<Chunk.LogEntry> openEntries = new ArrayList<>();
    private int openApproxBytes = 4;
    private ScheduledFuture<?> sealTimerFuture = null;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final Map<String, Set<String>> attemptedReplicas = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> pendingTargets = new ConcurrentHashMap<>();

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
            throw new IllegalStateException("Ficheiro master.key não encontrado em " + mkPath.toAbsolutePath());
        }
        byte[] mk = Files.readAllBytes(mkPath);
        this.crypto = new ChunkCrypto(mk, 1);
        this.placement = new Placement(Wire.ALFA, Wire.MARGEM_BYTES);
        this.registry = new ReplicaRegistry(baseDir);
        this.catalog = new Catalog(baseDir);
        this.hub = new PubSubHub(Wire.PORTO, registry, this::onReplicaMessage, this::logEvent);

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

    public void startServices() {
        hub.start();
        scheduler.scheduleAtFixedRate(
                () -> registry.atualizarEstados(System.currentTimeMillis(), this::logEvent),
                1000L, 1000L, TimeUnit.MILLISECONDS
        );
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
    // ESCRITA, LOG COM FSYNC, SELAGEM E COLOCAÇÃO REPLICADA
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

            Files.writeString(sealedDir.resolve(sealed.id + ".bin"),
                    sealed.keyVersion + ";" + saltB64 + ";" + nonceB64 + ";" + ctB64);

            catalog.adicionar(sealed.id, lsnDe, lsnAte, sealed.ciphertext.length);
            logEvent("SELADO", "chunk=" + sealed.id + " lsn=" + lsnDe + ".." + lsnAte + " bytes=" + sealed.ciphertext.length);

            attemptedReplicas.put(sealed.id, ConcurrentHashMap.newKeySet());
            pendingTargets.put(sealed.id, ConcurrentHashMap.newKeySet());
            enviarRonda(sealed.id, 1);
        } catch (Exception e) {
            logEvent("ERRO", "Falha ao selar chunk: " + e.getMessage());
        }
    }

    private void enviarRonda(String chunkId, int ronda) {
        try {
            Catalog.EntradaCatalogo entry = catalog.get(chunkId);
            if (entry == null) return;

            int rTarget = currentR.get();
            int confirmadas = entry.replicas.size();
            int emFalta = rTarget - confirmadas;
            if (emFalta <= 0) return;

            if (ronda > 1) {
                logEvent("RONDA", chunkId + " " + ronda);
            }

            Set<String> tentadas = attemptedReplicas.computeIfAbsent(chunkId, k -> ConcurrentHashMap.newKeySet());
            Set<String> exclude = new HashSet<>(tentadas);
            exclude.addAll(entry.replicas);

            List<String> chosen = registry.reserveAndChoose(chunkId, entry.tamanho, emFalta, exclude, placement);
            int previstas = confirmadas + chosen.size();
            if (previstas < rTarget) {
                logEvent("SUBREPLICADO", chunkId + " " + previstas + "/" + rTarget);
            }

            if (chosen.isEmpty()) {
                if (ronda < Wire.MAX_RONDAS) {
                    scheduler.schedule(() -> enviarRonda(chunkId, ronda + 1), Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                }
                return;
            }

            String storeMsg = buildStoreMessage(chunkId);
            if (storeMsg == null) return;

            Set<String> pending = pendingTargets.computeIfAbsent(chunkId, k -> ConcurrentHashMap.newKeySet());
            tentadas.addAll(chosen);
            pending.addAll(chosen);

            for (String repId : chosen) {
                hub.publish(Wire.topicReplica(repId), storeMsg);
                logEvent("STORE", chunkId + " -> " + repId);
            }

            scheduler.schedule(() -> onAckTimeout(chunkId, entry.tamanho, chosen, ronda),
                    Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logEvent("ERRO", "Erro na ronda de colocação: " + e.getMessage());
        }
    }

    private void onAckTimeout(String chunkId, long size, List<String> sentTargets, int ronda) {
        Set<String> pending = pendingTargets.get(chunkId);
        for (String repId : sentTargets) {
            if (pending != null && pending.remove(repId)) {
                registry.libertar(chunkId, repId, size);
            }
        }

        Catalog.EntradaCatalogo entry = catalog.get(chunkId);
        if (entry == null) return;

        int rTarget = currentR.get();
        if (entry.replicas.size() < rTarget) {
            if (ronda < Wire.MAX_RONDAS) {
                enviarRonda(chunkId, ronda + 1);
            } else {
                logEvent("SUBREPLICADO", chunkId + " " + entry.replicas.size() + "/" + rTarget);
            }
        }
    }

    private String buildStoreMessage(String chunkId) throws IOException {
        Path file = sealedDir.resolve(chunkId + ".bin");
        if (!Files.exists(file)) return null;
        String[] p = Files.readString(file, StandardCharsets.UTF_8).split(";", 4);
        String hdrPlain = p[0] + ";" + p[1] + ";" + p[2];
        String hdrB64 = Base64.getEncoder().encodeToString(hdrPlain.getBytes(StandardCharsets.UTF_8));
        String binB64 = p[3];
        return Wire.encodeStore(chunkId, hdrB64, binB64);
    }

    // ==========================================================================
    // MENSAGENS RECEBIDAS DAS RÉPLICAS NO TÓPICO tpf-lider
    // ==========================================================================
    private void onReplicaMessage(String authReplicaId, String raw) {
        try {
            String[] p = Wire.decode(raw);
            switch (p[0]) {
                case "HB" -> {
                    if (p.length < 4 || !authReplicaId.equals(p[1])) {
                        logEvent("ERRO", "HB inválido ou ID divergente de " + authReplicaId);
                        return;
                    }
                    long usados = Long.parseLong(p[2]);
                    long quota = Long.parseLong(p[3]);
                    registry.onHeartbeat(authReplicaId, usados, quota, this::logEvent);
                }
                case "INVENTARIO" -> {
                    if (p.length < 2 || !authReplicaId.equals(p[1])) {
                        logEvent("ERRO", "INVENTARIO inválido ou ID divergente de " + authReplicaId);
                        return;
                    }
                    int count = (p.length < 3 || p[2].isBlank()) ? 0 : p[2].split(",").length;
                    logEvent("INVENTARIO", authReplicaId + " chunks=" + count);
                }
                case "ACK" -> {
                    if (p.length < 4) {
                        logEvent("ERRO", "ACK malformado de " + authReplicaId);
                        return;
                    }
                    String msgId = p[1];
                    String chunkId = p[2];
                    String sha = p[3];

                    if (!authReplicaId.equals(msgId)) {
                        logEvent("ACK-REJEITADO", chunkId + " " + msgId + " id-ligacao(" + authReplicaId + ")");
                        return;
                    }

                    Catalog.EntradaCatalogo entry = catalog.get(chunkId);
                    Set<String> pending = pendingTargets.get(chunkId);
                    if (entry == null || pending == null || !pending.contains(authReplicaId)) {
                        logEvent("ACK-REJEITADO", chunkId + " " + authReplicaId + " store-nao-enviado");
                        return;
                    }

                    if (!chunkId.equalsIgnoreCase(sha)) {
                        pending.remove(authReplicaId);
                        registry.libertar(chunkId, authReplicaId, entry.tamanho);
                        logEvent("ACK-REJEITADO", chunkId + " " + authReplicaId + " hash-invalido");
                        return;
                    }

                    pending.remove(authReplicaId);
                    registry.confirmarReserva(chunkId, authReplicaId, entry.tamanho);
                    int quorum = Wire.quorumDuravel(currentR.get());
                    boolean durableNow = catalog.confirmar(chunkId, authReplicaId, quorum);
                    logEvent("ACK", chunkId + " " + authReplicaId);
                    if (durableNow) {
                        logEvent("DURAVEL", chunkId);
                    }
                }
                default -> logEvent("ERRO", "Mensagem desconhecida em tpf-lider de " + authReplicaId + ": " + raw);
            }
        } catch (Exception e) {
            logEvent("ERRO", "Erro ao tratar mensagem de réplica " + authReplicaId + ": " + e.getMessage());
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
        int quorum = Wire.quorumDuravel(rTarget);
        List<ReplicaRegistry.ReplicaInfo> reps = registry.all();
        List<Catalog.EntradaCatalogo> chunks = catalog.all();

        long selados = chunks.stream().filter(c -> "SELADO".equals(c.estado)).count();
        long duraveis = chunks.stream().filter(c -> "DURAVEL".equals(c.estado)).count();
        long subreplicados = chunks.stream().filter(c -> c.replicas.size() < rTarget).count();

        StringBuilder sb = new StringBuilder();
        sb.append("**Estado do Líder** | R = `").append(rTarget)
          .append("` | Quórum = `").append(quorum)
          .append("` | SELADO = `").append(selados)
          .append("` | DURAVEL = `").append(duraveis)
          .append("` | Subreplicados = `").append(subreplicados).append("`\n");

        if (reps.isEmpty()) {
            sb.append("_Nenhuma réplica registada._");
        } else {
            for (ReplicaRegistry.ReplicaInfo r : reps) {
                long nChunksCat = chunks.stream().filter(c -> c.replicas.contains(r.id)).count();
                double pct = r.quota > 0 ? (100.0 * r.usedBytes / r.quota) : 0.0;
                sb.append(String.format("• **%s** [%s] — %d / %d bytes (%.1f%%) | chunks no catálogo: %d\n",
                        r.id, r.monitor.getState(), r.usedBytes, r.quota, pct, nChunksCat));
            }
        }
        event.reply(sb.toString()).queue();
    }

    private void handleConfig(SlashCommandInteractionEvent event) {
        event.reply("Resposta provisória: `/config` ainda não disponível neste sprint.").setEphemeral(true).queue();
    }

    private void handleRecuperar(SlashCommandInteractionEvent event) {
        event.reply("Resposta provisória: `/recuperar` a partir das réplicas será implementado no próximo sprint.").setEphemeral(true).queue();
    }

    private void handleReplicaToken(SlashCommandInteractionEvent event) {
        String token = hub.gerarToken();
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
            hub.disconnectReplica(id);
            logEvent("REMOVIDO", "Réplica removida: " + id);
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