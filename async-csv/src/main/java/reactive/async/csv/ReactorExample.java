package reactive.async.csv;

        import org.reactivestreams.Publisher;
        import reactor.core.publisher.Flux;
        import java.nio.ByteBuffer;
        import java.util.List;

        public class ReactorExample {
            public static void main(String[] args) {
                System.out.println("Starting Reactor Example");

                EnhancedByteBufferProcessor<CsvResult> processor = new EnhancedByteBufferProcessor<>(
                        (buffer, leftover) -> CsvResult.fromByteBuffer(buffer, (byte) ',', leftover), // Use CsvResult::fromByteBuffer with leftover
                        EnhancedByteBufferProcessor.ErrorHandlingStrategy.SKIP_ON_ERROR,
                        () -> new CsvResult(List.of(List.of("Fallback".getBytes())), new byte[0]), // Provide a fallback CsvResult
                        (byte) ',' // CSV delimiter
                );

//                Flux<ByteBuffer> flux = Flux.just(
//                        ByteBuffer.wrap("name,age\nJohn,30\nAl".getBytes()),
//                        ByteBuffer.wrap("ice,25\nBob,40\n".getBytes())
//                );

                Flux<ByteBuffer> flux = Flux.just(
                        ByteBuffer.wrap("name,age,adress\njuergen,".getBytes()),
                        ByteBuffer.wrap("43,graz\n".getBytes())
                );

                Publisher<CsvResult> resultPublisher = processor.process(flux);
                Flux.from(resultPublisher).subscribe(System.out::println);

                System.out.println("Ending Reactor Example");
            }
        }