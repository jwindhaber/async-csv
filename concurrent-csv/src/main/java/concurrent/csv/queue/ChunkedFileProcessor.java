package concurrent.csv.queue;

import com.google.common.base.Stopwatch;

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
import java.util.concurrent.atomic.AtomicInteger;

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
    //private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final CsvLineConsumer consumer;

    public ChunkedFileProcessor(Path filePath, int chunkSize, int queueCapacity, CsvLineConsumer consumer) {
        this.filePath = filePath;
        this.chunkSize = chunkSize;
        this.queueCapacity = queueCapacity;
        this.futureQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.consumer = consumer;
        this.processorExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.readerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.writerExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void run() throws IOException, InterruptedException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();

            readerExecutor.submit(() -> {
                long position = 0;
                ByteBuffer leftover = ByteBuffer.allocate(0);

                try {
                    while (position < fileSize && !shutdown.get()) {
                        int readSize = (int) Math.min(chunkSize, fileSize - position);
                        int leftoverSize = leftover.remaining();
                        ByteBuffer buffer = ByteBuffer.allocate(leftoverSize + readSize);

                        //String decod = decoder.decode(leftover.duplicate()).toString();
                        //leftover.flip();
                        buffer.put(leftover);

                        //String decode = decoder.decode(leftover.duplicate()).toString();


                        // Read directly into buffer after leftover
                        int bytesRead = channel.read(buffer, position);
                        buffer.flip();

                        //String decode1 = decoder.decode(buffer.duplicate()).toString();

                        int lastCsvBoundary = findLastCompleteCsvRecord(buffer);
                        if (lastCsvBoundary == -1) {
                            leftover = buffer;
                            position += readSize;
                            continue;
                        }

                        ByteBuffer toProcess = buffer.slice(0, lastCsvBoundary);
                        leftover = buffer.slice(lastCsvBoundary, buffer.limit() - lastCsvBoundary);

                        final ByteBuffer chunkCopy = toProcess.asReadOnlyBuffer();


                        Chunk chunk = new Chunk(chunkCopy);
                        Future<ChunkResult> future = processorExecutor.submit(() -> {
                            try {
                                return process(chunk);
                            } catch (NonFatalProcessingException e) {
                                return new ChunkResult(null, Optional.of(e));
                            } catch (Exception fatal) {
                                shutdownAll();
                                throw fatal;
                            }
                        });
                        futureQueue.put(future);

                        position += readSize;
                    }

                    if (leftover.hasRemaining() && !shutdown.get()) {
                        Chunk chunk = new Chunk(leftover);
                        Future<ChunkResult> future = processorExecutor.submit(() -> {
                            try {
                                return process(chunk);
                            } catch (NonFatalProcessingException e) {
                                return new ChunkResult(null, Optional.of(e));
                            } catch (Exception fatal) {
                                shutdownAll();
                                throw fatal;
                            }
                        });
                        futureQueue.put(future);
                    }

                } catch (IOException | InterruptedException e) {
                    shutdownAll();
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
                    shutdownAll();
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

    private void shutdownAll() {
        System.out.println("Shutting down all executors...");
        if (shutdown.compareAndSet(false, true)) {
            processorExecutor.shutdownNow();
            readerExecutor.shutdownNow();
            writerExecutor.shutdownNow();
            futureQueue.clear();
            futureQueue.offer(poisonPill);
        }
    }

    public static int findLastCompleteCsvRecord(ByteBuffer buffer) {
        boolean inQuotes = false;
        for (int i = buffer.limit() - 1; i >= buffer.position(); i--) {
            byte b = buffer.get(i);
            if (b == '"') inQuotes = !inQuotes;
            if (b == '\n' && !inQuotes) return i + 1;
        }
        return -1;
    }

    private ChunkResult process(Chunk chunk) throws NonFatalProcessingException {
        ByteBuffer buffer = chunk.buffer();
        buffer.mark();

        CharBuffer charBuffer;
        try {
            charBuffer = StandardCharsets.UTF_8.newDecoder().decode(buffer);
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

        return new ChunkResult(records, Optional.empty());
    }

    private void handleResult(ChunkResult result) {

        consumer.accept(result);

    }


    private record Chunk(ByteBuffer buffer) {}
    public record ChunkResult(List<List<String>> buffer, Optional<Exception> error) {}

    public static class NonFatalProcessingException extends Exception {
        public NonFatalProcessingException(String message) {
            super(message);
        }

        public NonFatalProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public interface CsvLineConsumer {
        void accept(ChunkResult result);
    }

    public static void main(String[] args) throws Exception {
        //Path file = Path.of(ClassLoader.getSystemResource("inserts-1_000.csv").toURI());
        Path file = Path.of("C:\\repository\\async\\async-csv\\concurrent-csv\\build\\tmp\\jmh\\benchmark-12657088394399817139.csv");
        //ChunkedFileProcessor processor = new ChunkedFileProcessor(file, 64 * 1024 * 1024, 16);
        AtomicInteger recordsProcessed = new AtomicInteger();
        ChunkedFileProcessor processor = new ChunkedFileProcessor(file, 64 * 1024, 16,(result -> {
            // Process the result
            //System.out.println("Processed chunk with " + result.buffer.size() + " records.");
            recordsProcessed.addAndGet(result.buffer.size());
            result.error.ifPresent(err -> {
                System.err.println("Error processing chunk: " + err.getMessage());
            });
        }));
        Stopwatch stopwatch = Stopwatch.createStarted();
        processor.run();
        stopwatch.stop();
        System.out.println("Processing completed.");
        System.out.println("Processed with " + recordsProcessed.get() + " records.");
        System.out.println("Processing took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
    }
}
