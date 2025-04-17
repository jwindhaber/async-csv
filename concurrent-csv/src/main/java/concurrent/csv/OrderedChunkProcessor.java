package concurrent.csv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class OrderedChunkProcessor {

    public static <R> void readAndProcessChunks(
            Path filePath,
            int chunkSize,
            int maxConcurrent,
            Function<ByteBuffer, R> processChunk,
            Consumer<R> downstream
    ) throws IOException, InterruptedException {

        BlockingQueue<StructuredTaskScope.Subtask<R>> inFlight = new ArrayBlockingQueue<>(maxConcurrent);

        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
             var scope = new StructuredTaskScope.ShutdownOnFailure()) {

            // Consumer thread: continuously drain and emit results
            Thread.startVirtualThread(() -> {
                while (true) {
                    try {
                        var subtask = inFlight.take();
                        StructuredTaskScope.Subtask.State state = subtask.state();
                        R result = subtask.get(); // blocks until done
                        downstream.accept(result);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Throwable t) {
                        t.printStackTrace(); // or use server-side logging
                    }
                }
            });

            // Producer: read chunks and fork tasks
            long fileSize = channel.size();
            long position = 0;

            while (position < fileSize) {
                int size = (int) Math.min(chunkSize, fileSize - position);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                channel.read(buffer, position);
                buffer.flip();
                position += size;

                var subtask = scope.fork(() -> processChunk.apply(buffer));
                inFlight.put(subtask); // blocks if queue is full
            }

            scope.join();
            scope.throwIfFailed();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Path path = Paths.get("D:/tmp/sandbox/input-1.txt");
        int chunkSize = 1024;
        int maxConcurrent = 4;

        readAndProcessChunks(
                path,
                chunkSize,
                maxConcurrent,
                buffer -> "Processed " + buffer.remaining() + " bytes",
                result -> System.out.println(">> " + result)
        );

        Thread.sleep(1000);
    }
}

//    // 🔧 Example usage
//    public static void main(String[] args) throws Exception {
//        Path path = Paths.get("D:/tmp/sandbox/input-1.txt");
//        int chunkSize = 1024;
//        int maxConcurrent = 4;
//
//        AtomicInteger counter = new AtomicInteger(0);
//
//        System.out.println("starting");
//        readAndProcessChunks(
//                path,
//                chunkSize,
//                maxConcurrent,
//                buffer -> {
//                    System.out.println("test");
//                    return "Processed " + buffer.remaining() + " bytes";
//                },
//                result -> System.out.println(">> " + counter.incrementAndGet() + ": " + result)
//        );
//    }
//}
