package concurrent.csv.queue.upload;

import concurrent.csv.queue.ChunkedFileProcessor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.SubmissionPublisher;

public class CsvRowPublisherConsumer implements ChunkedFileProcessor.CsvLineConsumer {
    private final SubmissionPublisher<ByteBuffer> publisher;

    public CsvRowPublisherConsumer(SubmissionPublisher<ByteBuffer> publisher) {
        this.publisher = publisher;
    }

    @Override
    public void accept(ChunkedFileProcessor.ChunkResult result) {
        if (result.error().isPresent()) {
            System.err.println("Error processing chunk: " + result.error().get());
            return;
        }

        for (ChunkedFileProcessor.Row row : result.rows()) {
            // Example: Convert Row to CSV line (basic implementation)
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.getFieldCount(); i++) {
                ChunkedFileProcessor.Field field = row.getFields()[i];
                // Real logic: slice from CharBuffer for perf, here we simulate with placeholder
                sb.append("value").append(i);
                if (i < row.getFieldCount() - 1) sb.append(",");
            }
            sb.append("\n");
            ByteBuffer buffer = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
            publisher.submit(buffer); // backpressure-aware
        }
    }
}
