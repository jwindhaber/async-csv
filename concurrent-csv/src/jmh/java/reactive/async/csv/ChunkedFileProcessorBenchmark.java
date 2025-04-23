package reactive.async.csv;

import concurrent.csv.queue.ChunkedFileProcessor;
import concurrent.csv.virtual.ParallelCsvParser;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

//@BenchmarkMode(Mode.Throughput)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ChunkedFileProcessorBenchmark {

    private Path tempFile;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempFile = Files.createTempFile("benchmark-", ".csv");
        System.out.println("Temp file created: " + tempFile.toAbsolutePath());

        try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
            // Write a large CSV file with 1 million lines
            int lines = 1_000_000;

            for (int i = 0; i < lines; i++) {
                //writer.write("field1,field2,\"quoted\nfield3\",field4\r\n");
                writer.write("CY-1-2,true,CZ-1,2025-01-27T22:45:11Z,CESOP701,CESOP701,1113441.00,EUR,Bank transfer,TODO,true,DE,OBAN,Four party card scheme,other,c22a7a57-344a-4985-b99f-0421dd80ccbb\n");
            }
        }


    }

    @Benchmark
    public void benchmarkCsvParsing(Blackhole bh) throws Exception {
        ChunkedFileProcessor processor = new ChunkedFileProcessor(tempFile, 64 * 1024, 16, (result) -> bh.consume(result));
        processor.run();
    }

    @TearDown(Level.Trial)
    public void cleanup() throws IOException {
        //Files.deleteIfExists(tempFile);
    }
}
