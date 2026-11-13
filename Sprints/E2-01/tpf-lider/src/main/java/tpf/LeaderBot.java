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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

public class LeaderBot extends ListenerAdapter {

    private static final String ADMIN_ROLE = "admin-arquivo";
    private static final String ARCHIVE_CHANNEL = "arquivo";
    private static final Set<String> ADMIN_COMMANDS = Set.of(
            "config", "replica-token", "replica-remover", "rajada"
    );

    private final String guildId;

    public LeaderBot(String guildId) {
        this.guildId = guildId;
    }

    public static void main(String[] args) throws Exception {
        String token = loadConfig("DISCORD_TOKEN", "token.txt");
        String guildId = loadConfig("DISCORD_GUILD_ID", "guild.txt");

        if (token == null || token.isBlank()) {
            System.err.println("Erro: Token não encontrado. Define a variável DISCORD_TOKEN ou cria o ficheiro token.txt.");
            System.exit(1);
        }
        if (guildId == null || guildId.isBlank()) {
            System.err.println("Erro: ID do servidor não encontrado. Define a variável DISCORD_GUILD_ID ou cria o ficheiro guild.txt.");
            System.exit(1);
        }

        JDA jda = JDABuilder.createDefault(token.trim())
                .enableIntents(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot(guildId.trim()))
                .build();

        jda.awaitReady();
    }

    private static String loadConfig(String envVar, String fallbackFile) {
        String val = System.getenv(envVar);
        if (val != null && !val.isBlank()) {
            return val;
        }
        try {
            Path path = Path.of(fallbackFile);
            if (Files.exists(path)) {
                return Files.readString(path).trim();
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    @Override
    public void onReady(ReadyEvent event) {
        Guild guild = event.getJDA().getGuildById(guildId);
        if (guild == null) {
            System.err.println("Erro: Servidor com ID " + guildId + " não encontrado. Verifica se o bot foi convidado.");
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
                cmds -> System.out.println("LeaderBot online! " + cmds.size() + " comandos registados no servidor '" + guild.getName() + "'."),
                err -> System.err.println("Erro ao registar comandos: " + err.getMessage())
        );
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String command = event.getName();

        // Verificação de permissões para comandos de administração
        if (ADMIN_COMMANDS.contains(command) && !hasAdminRole(event.getMember())) {
            event.reply("Permissão negada: apenas membros com o papel `" + ADMIN_ROLE + "` podem executar `/" + command + "`.")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        switch (command) {
            case "registar" -> handleRegistar(event);
            case "estado" -> event.reply("/estado recebido — ainda não implementado").queue();
            case "config" -> handleConfig(event);
            case "recuperar" -> handleRecuperar(event);
            case "replica-token" -> event.reply("/replica-token recebido — ainda não implementado").queue();
            case "replica-remover" -> handleReplicaRemover(event);
            case "rajada" -> handleRajada(event);
            default -> event.reply("Comando desconhecido: /" + command).setEphemeral(true).queue();
        }
    }

    private void handleRegistar(SlashCommandInteractionEvent event) {
        String texto = getStringOption(event, "texto");
        if (texto == null || texto.isBlank()) {
            event.reply("Erro: o parâmetro `texto` não pode estar vazio.").setEphemeral(true).queue();
            return;
        }
        event.reply("/registar recebido: texto=\"" + texto + "\" — ainda não implementado").queue();
    }

    private void handleConfig(SlashCommandInteractionEvent event) {
        String sub = event.getSubcommandName();
        if (!"r".equals(sub)) {
            event.reply("Erro: subcomando inválido em `/config`.").setEphemeral(true).queue();
            return;
        }

        String rawN = getStringOption(event, "n");
        int n;
        try {
            n = Integer.parseInt(rawN != null ? rawN.trim() : "");
        } catch (NumberFormatException e) {
            event.reply("Erro: argumento inválido em `/config r` — `n` deve ser um número inteiro (recebido: `" + rawN + "`).")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        if (n <= 0) {
            event.reply("Erro: argumento inválido em `/config r` — `n` deve ser maior que 0 (recebido: `" + n + "`).")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        event.reply("/config r recebido: n=" + n + " — ainda não implementado").queue();
    }

    private void handleRecuperar(SlashCommandInteractionEvent event) {
        OptionMapping optDe = event.getOption("de");
        OptionMapping optAte = event.getOption("ate");
        if (optDe == null || optAte == null) {
            event.reply("Erro: os parâmetros `de` e `ate` são obrigatórios.").setEphemeral(true).queue();
            return;
        }

        long de = optDe.getAsLong();
        long ate = optAte.getAsLong();

        if (de < 1 || ate < de) {
            event.reply("Erro: intervalo inválido em `/recuperar` (de=" + de + ", ate=" + ate + "). Deve cumprir `1 <= de <= ate`.")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        event.reply("/recuperar recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
    }

    private void handleReplicaRemover(SlashCommandInteractionEvent event) {
        String id = getStringOption(event, "id");
        if (id == null || id.isBlank()) {
            event.reply("Erro: o identificador `id` da réplica não pode estar vazio.").setEphemeral(true).queue();
            return;
        }
        event.reply("/replica-remover recebido: id=" + id + " — ainda não implementado").queue();
    }

    private void handleRajada(SlashCommandInteractionEvent event) {
        OptionMapping optN = event.getOption("n");
        OptionMapping optIntervalo = event.getOption("intervaloms");
        if (optN == null || optIntervalo == null) {
            event.reply("Erro: os parâmetros `n` e `intervaloms` são obrigatórios.").setEphemeral(true).queue();
            return;
        }

        long n = optN.getAsLong();
        long intervaloMs = optIntervalo.getAsLong();

        if (n <= 0 || intervaloMs < 0) {
            event.reply("Erro: argumentos inválidos em `/rajada` (n=" + n + ", intervaloms=" + intervaloMs + "). Deve cumprir `n > 0` e `intervaloms >= 0`.")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        event.reply("/rajada recebido: n=" + n + ", intervaloms=" + intervaloMs + " — ainda não implementado").queue();
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot() || !event.isFromGuild()) {
            return;
        }

        if (!ARCHIVE_CHANNEL.equals(event.getChannel().getName())) {
            return;
        }

        String autor = event.getAuthor().getName();
        String conteudo = event.getMessage().getContentDisplay();

        System.out.println("[#arquivo] " + autor + ": " + conteudo);
        event.getMessage()
                .reply("Mensagem recebida em #arquivo de **" + autor + "**: `" + conteudo + "` — registo ainda não implementado.")
                .queue();
    }

    private boolean hasAdminRole(Member member) {
        if (member == null) {
            return false;
        }
        return member.getRoles().stream()
                .anyMatch(role -> ADMIN_ROLE.equals(role.getName()));
    }

    private String getStringOption(SlashCommandInteractionEvent event, String name) {
        OptionMapping opt = event.getOption(name);
        return opt != null ? opt.getAsString() : null;
    }
}