package concurrent.csv.virtual;


import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class ParallelCsvParser {

    public record ByteSlice(ByteBuffer buffer, int start, int end) {
        public byte[] copy() {
            byte[] bytes = new byte[end - start];
            int originalPos = buffer.position();
            buffer.position(start);
            buffer.get(bytes, 0, end - start);
            buffer.position(originalPos);
            return bytes;
        }

        public String asString() {
            return new String(copy());
        }
    }

    public interface CsvLineConsumer {
        void accept(List<ByteSlice> fields, long lineNumber);
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

            // Read directly into buffer after leftover
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
        int start = chunk.position();
        boolean inQuotes = false;
        long line = startLineNumber;

        for (int i = chunk.position(); i < chunk.limit(); i++) {
            byte b = chunk.get(i);
            if (b == '"') {
                if (inQuotes && i + 1 < chunk.limit() && chunk.get(i + 1) == '"') {
                    i++; // escaped quote
                } else {
                    inQuotes = !inQuotes;
                }
            }
            if (b == '\n' && !inQuotes) {
                int end = (i > chunk.position() && chunk.get(i - 1) == '\r') ? i - 1 : i;
                List<ByteSlice> fields = parseCsvLine(chunk, start, end);
                consumer.accept(fields, line++);
                start = i + 1;
            }
        }
        if (start < chunk.limit()) {
            List<ByteSlice> fields = parseCsvLine(chunk, start, chunk.limit());
            consumer.accept(fields, line);
        }
    }

    private static List<ByteSlice> parseCsvLine(ByteBuffer buffer, int from, int to) {
        List<ByteSlice> fields = new ArrayList<>();
        int start = from;
        boolean inQuotes = false;

        for (int i = from; i <= to; i++) {
            boolean atEnd = i == to;
            byte b = atEnd ? (byte) ',' : buffer.get(i);

            if (b == '"') {
                if (inQuotes && i + 1 < buffer.limit() && buffer.get(i + 1) == '"') {
                    i++; // escaped quote
                } else {
                    inQuotes = !inQuotes;
                }
            }
            if ((b == ',' && !inQuotes) || atEnd) {
                int fieldEnd = i;
                ByteSlice slice = unquote(buffer, start, fieldEnd);
                fields.add(slice);
                start = i + 1;
            }
        }
        return fields;
    }

    private static ByteSlice unquote(ByteBuffer buffer, int start, int end) {
        if (end - start >= 2 && buffer.get(start) == '"' && buffer.get(end - 1) == '"') {
            return new ByteSlice(buffer, start + 1, end - 1);
        }
        return new ByteSlice(buffer, start, end);
    }

    private static int countCsvLines(ByteBuffer buffer) {
        boolean inQuotes = false;
        int count = 0;
        for (int i = buffer.position(); i < buffer.limit(); i++) {
            byte b = buffer.get(i);
            if (b == '"') {
                if (inQuotes && i + 1 < buffer.limit() && buffer.get(i + 1) == '"') {
                    i++; // escaped quote
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
        //Path file = Path.of("large.csv");
        //Path file = Path.of("D:\\cesop\\001-testdata\\inserts-1_000.csv");
        //Path file = Path.of(ClassLoader.getSystemResource("inserts-1_000.csv").toURI());

        Path file = setup();
        parseCsvFile(file, 64 * 1024, Runtime.getRuntime().availableProcessors(), (fields, line) -> {
            System.out.printf("Line %d: %s%n", line, fields.get(0).asString()); // example
        });
    }

    public static Path setup() throws IOException {
        Path tempFile = Files.createTempFile("benchmark-", ".csv");
        System.out.println("Temp file created: " + tempFile.toAbsolutePath());

        try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
            // Write a large CSV file with 1 million lines
            int lines = 1_000_000;

            for (int i = 0; i < lines; i++) {
                //writer.write("field1,field2,\"quoted\nfield3\",field4\r\n");
                writer.write("CY-1-2,true,CZ-1,2025-01-27T22:45:11Z,CESOP701,CESOP701,1113441.00,EUR,Bank transfer,TODO,true,DE,OBAN,Four party card scheme,other,c22a7a57-344a-4985-b99f-0421dd80ccbb\n");
            }
        }
        return tempFile;


    }
}
