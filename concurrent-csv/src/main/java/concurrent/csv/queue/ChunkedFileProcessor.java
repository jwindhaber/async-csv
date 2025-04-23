package concurrent.csv.queue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
        // TODO: actual processing logic
        return new ChunkResult(chunk.buffer(), Optional.empty());
    }

    private void handleResult(ChunkResult result) {
        // TODO: handle the result downstream, e.g., emit to another system
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
