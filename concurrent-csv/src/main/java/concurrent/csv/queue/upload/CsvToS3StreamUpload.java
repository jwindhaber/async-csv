package concurrent.csv.queue.upload;

import concurrent.csv.queue.ChunkedFileProcessor;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

public class CsvToS3StreamUpload {
    public static void main(String[] args) throws Exception {
        Path inputCsv = Path.of("your-input.csv"); // change to actual file

        S3AsyncClient s3 = S3AsyncClient.create();
        ExecutorService publisherExecutor = Executors.newFixedThreadPool(2);
        SubmissionPublisher<ByteBuffer> publisher = new SubmissionPublisher<>(publisherExecutor, 64);

//TODO check if this is correct
        Publisher<ByteBuffer> genPublisher = FlowAdapters.toPublisher(publisher);

        // Hook up publisher to S3
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket("your-bucket")
                .key("your-object-key.csv")
                .build();

        s3.putObject(request, AsyncRequestBody.fromPublisher(genPublisher))
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        System.err.println("Upload failed: " + err.getMessage());
                    } else {
                        System.out.println("Upload complete: " + resp);
                    }
                    publisher.close();
                    s3.close();
                    publisherExecutor.shutdown();
                });

        // Start the processor with the streaming consumer
        ChunkedFileProcessor processor = new ChunkedFileProcessor(
                inputCsv,
                64 * 1024,
                16,
                new CsvRowPublisherConsumer(publisher)
        );

        processor.run(); // runs parsing and validation
    }
}

