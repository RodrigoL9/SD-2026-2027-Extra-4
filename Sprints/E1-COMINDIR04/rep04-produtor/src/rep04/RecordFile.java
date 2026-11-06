package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class RecordFile {

    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(
                path,
                r.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    public synchronized long lastSeq() throws IOException {
        SensorRecord last = last();

        if (last == null) {
            return 0;
        }

        return last.getSeq();
    }

    // Devolve o último registo do ficheiro ou null se estiver vazio/não existir
    public synchronized SensorRecord last() throws IOException {

        if (!Files.exists(path)) {
            return null;
        }

        SensorRecord ultimo = null;

        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                ultimo = SensorRecord.fromLine(line);
            }
        }

        return ultimo;
    }
}
