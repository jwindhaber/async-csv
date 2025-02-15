package reactive.async.csv;

import com.google.common.base.Stopwatch;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReactorExampleFile {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Reactor Example");

        EnhancedByteBufferProcessor<CsvResult> processor = new EnhancedByteBufferProcessor<>(
                (buffer, leftover) -> CsvResult.fromByteBuffer(buffer, (byte) ',', leftover), // Use CsvResult::fromByteBuffer with leftover
                EnhancedByteBufferProcessor.ErrorHandlingStrategy.SKIP_ON_ERROR,
                () -> new CsvResult(List.of(List.of("Fallback".getBytes())), ByteBuffer.allocate(0)), // Provide a fallback CsvResult
                (byte) ',' // CSV delimiter
        );


        String filePath = "D:\\cesop\\001-testdata\\inserts-10_000_000.csv";
//        String filePath = "D:\\cesop\\001-testdata\\inserts-1_000.csv";
//        Flux<ByteBuffer> byteBufferFlux = readFileToFlux(filePath);
        List<ByteBuffer> input = generateInputData();
        Flux<ByteBuffer> byteBufferFlux = Flux.fromIterable(input);

        Stopwatch started = Stopwatch.createStarted();
        Publisher<CsvResult> resultPublisher = processor.process(byteBufferFlux);
        Flux.from(resultPublisher)
                .flatMap(csvResult -> Flux.fromIterable(csvResult.getLines()))
//                 .map(line -> line.stream()
//                        .map(String::new)
//                        .toList())
//                .count()
                .subscribe();
        started.stop();
        System.out.println(started.toString());
        System.out.println("Ending Reactor Example");

    }


    private static final int DEFAULT_BUFFER_SIZE = 8192; // Default ByteBuffer size


    private static List<ByteBuffer> generateInputDataStitched() {
        try {
            Path path = Paths.get("D:\\cesop\\001-testdata\\inserts-1_000.csv");
            byte[] fileBytes = Files.readAllBytes(path);
            List<byte[]> byteBuffers = new ArrayList<>();
            int bufferSize = 8192;
            for (int i = 0; i < fileBytes.length; i += bufferSize) {
                int end = Math.min(fileBytes.length, i + bufferSize);
                byteBuffers.add(Arrays.copyOfRange(fileBytes, i, end));
            }
            List<ByteBuffer> cumulatedList = new ArrayList<>();
            int n = 1000; // Number of times to add the list
            for (int i = 0; i < n; i++) {
                byteBuffers.stream()
                        .map(ByteBuffer::wrap)
                        .forEach(cumulatedList::add);
            }
            return cumulatedList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<ByteBuffer> generateInputData() {
        try {
            Path path = Paths.get("D:\\cesop\\001-testdata\\inserts-10_000_000.csv");
            byte[] fileBytes = Files.readAllBytes(path);
            List<ByteBuffer> byteBuffers = new ArrayList<>();
            int bufferSize = DEFAULT_BUFFER_SIZE;
            for (int i = 0; i < fileBytes.length; i += bufferSize) {
                int end = Math.min(fileBytes.length, i + bufferSize);
                byteBuffers.add(ByteBuffer.wrap(Arrays.copyOfRange(fileBytes, i, end)));
            }
            return byteBuffers;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }





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