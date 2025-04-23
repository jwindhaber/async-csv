package concurrent.csv.queue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkedFileProcessor {

    private final Path filePath;
    private final int chunkSize;
    private final int queueCapacity;
    private final BlockingQueue<Future<ChunkResult>> futureQueue;
    private final ExecutorService processorExecutor;
    private final ExecutorService readerExecutor;
    private final ExecutorService writerExecutor;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Future<ChunkResult> poisonPill = CompletableFuture.completedFuture(null);
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

    public ChunkedFileProcessor(Path filePath, int chunkSize, int queueCapacity) {
        this.filePath = filePath;
        this.chunkSize = chunkSize;
        this.queueCapacity = queueCapacity;
        this.futureQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.processorExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.readerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.writerExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void run() throws IOException, InterruptedException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();

            readerExecutor.submit(() -> {
                long position = 0;
                try {
                    while (position < fileSize && !shutdown.get()) {
                        long remaining = fileSize - position;
                        int size = (int) Math.min(chunkSize, remaining);
                        ByteBuffer buffer = ByteBuffer.allocate(size);
                        int read = channel.read(buffer, position);
                        if (read == -1) break;
                        buffer.flip();

                        Chunk chunk = new Chunk(buffer);
                        Future<ChunkResult> future = processorExecutor.submit(() -> {
                            try {
                                return process(chunk);
                            } catch (NonFatalProcessingException e) {
                                return new ChunkResult(chunk.buffer(), Optional.of(e));
                            } catch (Exception fatal) {
                                shutdown.set(true);
                                throw fatal;
                            }
                        });
                        futureQueue.put(future);

                        position += read;
                    }
                } catch (IOException | InterruptedException e) {
                    shutdown.set(true);
                    e.printStackTrace();
                } finally {
                    try {
                        futureQueue.put(poisonPill);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            writerExecutor.submit(() -> {
                try {
                    while (!shutdown.get()) {
                        Future<ChunkResult> future = futureQueue.take();
                        if (future == poisonPill) break;

                        ChunkResult result = future.get();
                        handleResult(result);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    shutdown.set(true);
                    e.printStackTrace();
                }
            });

            readerExecutor.shutdown();
            writerExecutor.shutdown();
            readerExecutor.awaitTermination(1, TimeUnit.MINUTES);
            writerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } finally {
            processorExecutor.shutdown();
            processorExecutor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    private ChunkResult process(Chunk chunk) throws NonFatalProcessingException {
        ByteBuffer buffer = chunk.buffer();
        buffer.mark();

        CharBuffer charBuffer;
        try {
            charBuffer = decoder.decode(buffer);
        } catch (CharacterCodingException e) {
            throw new NonFatalProcessingException("Decoding failed", e);
        }
        buffer.reset();

        List<List<String>> records = new ArrayList<>();
        List<String> currentRecord = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        boolean wasQuoted = false;

        for (int i = 0; i < charBuffer.length(); i++) {
            char c = charBuffer.get(i);

            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < charBuffer.length() && charBuffer.get(i + 1) == '"') {
                        currentField.append('"');
                        i++; // skip the escaped quote
                    } else {
                        inQuotes = false;
                        wasQuoted = true;
                    }
                } else {
                    currentField.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    currentRecord.add(currentField.toString());
                    currentField.setLength(0);
                    wasQuoted = false;
                } else if (c == '\n' || c == '\r') {
                    if (c == '\r' && i + 1 < charBuffer.length() && charBuffer.get(i + 1) == '\n') {
                        i++; // skip LF after CR
                    }
                    currentRecord.add(currentField.toString());
                    currentField.setLength(0);
                    records.add(currentRecord);
                    currentRecord = new ArrayList<>();
                    wasQuoted = false;
                } else {
                    currentField.append(c);
                }
            }
        }

        if (currentField.length() > 0 || wasQuoted) {
            currentRecord.add(currentField.toString());
        }
        if (!currentRecord.isEmpty()) {
            records.add(currentRecord);
        }

        return new ChunkResult(chunk.buffer(), Optional.empty());
    }

    private void handleResult(ChunkResult result) {
        result.error().ifPresent(err -> {
            System.err.println("Non-fatal error in chunk: " + err.getMessage());
        });
    }

    public void shutdown() {
        shutdown.set(true);
    }

    private record Chunk(ByteBuffer buffer) {}
    private record ChunkResult(ByteBuffer buffer, Optional<Exception> error) {}

    public static class NonFatalProcessingException extends Exception {
        public NonFatalProcessingException(String message) {
            super(message);
        }

        public NonFatalProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static void main(String[] args) throws Exception {
        Path file = Path.of("/path/to/your/largefile.dat");
        ChunkedFileProcessor processor = new ChunkedFileProcessor(file, 64 * 1024 * 1024, 16);
        processor.run();
    }
}
