package reactive.async.csv;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class CsvBenchmark {


    private List<ByteBuffer> inputData;


    @Setup(Level.Invocation) // Use Level.Trial if the data is expensive to create and should persist across iterations
    public void prepare() {
        inputData = generateInputDataStitched();
    }

    @Benchmark
    public void fibClassic(Blackhole bh) {

        EnhancedByteBufferProcessor<CsvResult> processor = new EnhancedByteBufferProcessor<>(
                (buffer, leftover) -> CsvResult.fromByteBuffer(buffer, (byte) ',', leftover), // Use CsvResult::fromByteBuffer with leftover
                EnhancedByteBufferProcessor.ErrorHandlingStrategy.SKIP_ON_ERROR,
                () -> new CsvResult(List.of(List.of("Fallback".getBytes())), ByteBuffer.allocate(0)), // Provide a fallback CsvResult
                (byte) ','
        );


        Flux<ByteBuffer> flux = Flux.fromIterable(inputData);
        Publisher<CsvResult> resultPublisher = processor.process(flux);
        Disposable subscribe = Flux.from(resultPublisher)
                .flatMap(csvResult -> Flux.fromIterable(csvResult.getLines()))
//                .map(line -> line.stream()
//                        .map(String::new)
//                        .toList())
//                .count()
                .subscribe();

        bh.consume(subscribe);
    }


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



}
