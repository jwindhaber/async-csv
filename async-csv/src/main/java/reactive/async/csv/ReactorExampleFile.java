package reactive.async.csv;

import com.google.common.base.Stopwatch;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ReactorExampleFile {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Reactor Example");

        EnhancedByteBufferProcessor<CsvResult> processor = new EnhancedByteBufferProcessor<>(
                (buffer, leftover) -> CsvResult.fromByteBuffer(buffer, (byte) ',', leftover), // Use CsvResult::fromByteBuffer with leftover
                EnhancedByteBufferProcessor.ErrorHandlingStrategy.SKIP_ON_ERROR,
                () -> new CsvResult(List.of(List.of("Fallback".getBytes())), new byte[0]), // Provide a fallback CsvResult
                (byte) ',' // CSV delimiter
        );


        String filePath = "D:\\cesop\\001-testdata\\inserts-10_000_000.csv";
//        String filePath = "D:\\cesop\\001-testdata\\inserts-1_000.csv";
        Flux<ByteBuffer> byteBufferFlux = readFileToFlux(filePath);

        Stopwatch started = Stopwatch.createStarted();
        Publisher<CsvResult> resultPublisher = processor.process(byteBufferFlux);
        Flux.from(resultPublisher)
                .flatMap(csvResult -> Flux.fromIterable(csvResult.getLines()))
                .count()
                .subscribe(System.out::println);
        started.stop();
        System.out.println(started.toString());
        System.out.println("Ending Reactor Example");

    }


    private static final int DEFAULT_BUFFER_SIZE = 8192; // Default ByteBuffer size

    public static Flux<ByteBuffer> readFileToFlux(String filePath)  {
        Path path = Paths.get(filePath);

        return Flux.generate(() -> Files.newInputStream(path), (inputStream, sink) -> {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
                int bytesRead = inputStream.read(buffer.array());
                if (bytesRead == -1) {
                    sink.complete();
                } else {
                    buffer.limit(bytesRead);
                    sink.next(buffer);
                }
            } catch (IOException e) {
                sink.error(e);
            }
            return inputStream;
        }, inputStream -> {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}