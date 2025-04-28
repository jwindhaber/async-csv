package concurrent.csv.virtual;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ParallelCsvParser {

    public record CharSlice(CharBuffer buffer, int start, int end) {
        public String asString() {
            CharSequence cs = buffer.subSequence(start, end);
            return cs.toString();
        }
    }

    public interface CsvLineConsumer {
        void accept(CharSlice[] fields, int fieldStart, int fieldCount, long lineNumber);
    }

    public static void parseCsvFile(
            Path file,
            int chunkSize,
            int parallelism,
            CsvLineConsumer consumer
    ) throws IOException, InterruptedException {

        ExecutorService processorPool = Executors.newVirtualThreadPerTaskExecutor();
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        long fileSize = channel.size();

        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        long position = 0;
        ByteBuffer leftover = ByteBuffer.allocate(0);
        long lineNumber = 0;

        while (position < fileSize) {
            int readSize = (int) Math.min(chunkSize, fileSize - position);
            int leftoverSize = leftover.remaining();
            ByteBuffer buffer = ByteBuffer.allocate(leftoverSize + readSize);

            leftover.flip();
            buffer.put(leftover);

            int bytesRead = channel.read(buffer, position);
            buffer.flip();

            int lastCsvBoundary = findLastCompleteCsvRecord(buffer);
            if (lastCsvBoundary == -1) {
                leftover = buffer;
                position += readSize;
                continue;
            }

            ByteBuffer toProcess = buffer.slice(0, lastCsvBoundary);
            leftover = buffer.slice(lastCsvBoundary, buffer.limit() - lastCsvBoundary);

            final long currentLineNumber = lineNumber;
            final ByteBuffer chunkCopy = toProcess.asReadOnlyBuffer();

            tasks.add(CompletableFuture.runAsync(() -> {
                processChunk(chunkCopy, currentLineNumber, consumer);
            }, processorPool));

            position += readSize;
            lineNumber += countCsvLines(toProcess);
        }

        if (leftover.remaining() > 0) {
            final long currentLineNumber = lineNumber;
            final ByteBuffer finalLeftover = leftover.asReadOnlyBuffer();
            tasks.add(CompletableFuture.runAsync(() -> {
                processChunk(finalLeftover, currentLineNumber, consumer);
            }, processorPool));
        }

        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        processorPool.shutdown();
    }

    private static void processChunk(ByteBuffer chunk, long startLineNumber, CsvLineConsumer consumer) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);

        CharBuffer charBuffer;
        try {
            charBuffer = decoder.decode(chunk);
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Failed to decode chunk", e);
        }

        int start = charBuffer.position();
        boolean inQuotes = false;
        long line = startLineNumber;
        CharSlice[] fields = new CharSlice[128];
        int fieldCount = 0;

        for (int i = charBuffer.position(); i < charBuffer.limit(); i++) {
            char c = charBuffer.get(i);
            if (c == '"') {
                if (inQuotes && i + 1 < charBuffer.limit() && charBuffer.get(i + 1) == '"') {
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            }
            if (c == '\n' && !inQuotes) {
                int end = (i > charBuffer.position() && charBuffer.get(i - 1) == '\r') ? i - 1 : i;
                fieldCount = parseCsvLine(charBuffer, start, end, fields);
                consumer.accept(fields, 0, fieldCount, line++);
                start = i + 1;
            }
        }
        if (start < charBuffer.limit()) {
            fieldCount = parseCsvLine(charBuffer, start, charBuffer.limit(), fields);
            consumer.accept(fields, 0, fieldCount, line);
        }
    }

    private static int parseCsvLine(CharBuffer buffer, int from, int to, CharSlice[] fields) {
        int start = from;
        boolean inQuotes = false;
        int fieldIndex = 0;

        for (int i = from; i <= to; i++) {
            boolean atEnd = i == to;
            char c = atEnd ? ',' : buffer.get(i);

            if (c == '"') {
                if (inQuotes && i + 1 < buffer.limit() && buffer.get(i + 1) == '"') {
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            }
            if ((c == ',' && !inQuotes) || atEnd) {
                int fieldEnd = i;
                fields[fieldIndex++] = unquote(buffer, start, fieldEnd);
                start = i + 1;
            }
        }
        return fieldIndex;
    }

    private static CharSlice unquote(CharBuffer buffer, int start, int end) {
        if (end - start >= 2 && buffer.get(start) == '"' && buffer.get(end - 1) == '"') {
            return new CharSlice(buffer, start + 1, end - 1);
        }
        return new CharSlice(buffer, start, end);
    }

    private static int countCsvLines(ByteBuffer buffer) {
        boolean inQuotes = false;
        int count = 0;
        for (int i = buffer.position(); i < buffer.limit(); i++) {
            byte b = buffer.get(i);
            if (b == '"') {
                if (inQuotes && i + 1 < buffer.limit() && buffer.get(i + 1) == '"') {
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            }
            if (b == '\n' && !inQuotes) count++;
        }
        return count;
    }

    private static int findLastCompleteCsvRecord(ByteBuffer buffer) {
        boolean inQuotes = false;
        for (int i = buffer.limit() - 1; i >= buffer.position(); i--) {
            byte b = buffer.get(i);
            if (b == '"') inQuotes = !inQuotes;
            if (b == '\n' && !inQuotes) return i + 1;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Path file = Path.of(ClassLoader.getSystemResource("inserts-1_000.csv").toURI());

        parseCsvFile(file, 64 * 1024, Runtime.getRuntime().availableProcessors(), (fields, startIdx, count, line) -> {
            System.out.printf("Line %d: %s%n", line, fields[startIdx].asString());
        });
    }
}
