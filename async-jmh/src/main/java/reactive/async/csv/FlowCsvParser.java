package reactive.async.csv;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class FlowCsvParser implements ReactiveCsvParser {

    private static final int BUFFER_SIZE = 8192;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Flow.Publisher<List<byte[]>> parse(Flow.Publisher<ByteBuffer> byteBufferStream, byte delimiter) {
        return subscriber -> {
            SubmissionPublisher<List<byte[]>> publisher = new SubmissionPublisher<>(executor, Flow.defaultBufferSize());
            AtomicReference<byte[]> leftover = new AtomicReference<>(new byte[0]); // Store leftover data between buffers

            byteBufferStream.subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription subscription;
                private boolean inQuotes = false;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(1);
                }

                @Override
                public void onNext(ByteBuffer buffer) {
                    try {
                        String bufferContent = new String(buffer.array(), buffer.position(), buffer.remaining());
                        System.out.println("Buffer: " + bufferContent);

                        List<List<byte[]>> rows = new ArrayList<>();
                        List<byte[]> currentRow = new ArrayList<>();
                        ByteBuffer fieldBuffer = ByteBuffer.allocate(BUFFER_SIZE);
                        byte[] combinedBuffer = new byte[leftover.get().length + buffer.remaining()];

                        System.arraycopy(leftover.get(), 0, combinedBuffer, 0, leftover.get().length);
                        buffer.get(combinedBuffer, leftover.get().length, buffer.remaining());

                        for (byte b : combinedBuffer) {
                            if (b == '"') {
                                inQuotes = !inQuotes;
                            } else if (!inQuotes && (b == delimiter || b == '\n' || b == '\r')) {
                                fieldBuffer.flip();
                                byte[] field = new byte[fieldBuffer.limit()];
                                fieldBuffer.get(field);
                                currentRow.add(field);
                                fieldBuffer.clear();

                                if (b == '\n' || (b == '\r' && buffer.hasRemaining() && buffer.get(buffer.position()) == '\n')) {
                                    rows.add(currentRow);
                                    currentRow = new ArrayList<>();
                                }
                            } else {
                                fieldBuffer.put(b);
                            }
                        }

                        byte[] newLeftover = new byte[fieldBuffer.position()];
                        fieldBuffer.flip();
                        fieldBuffer.get(newLeftover);
                        leftover.set(newLeftover);

                        rows.forEach(publisher::submit);
                        subscription.request(1);
                    } catch (Exception e) {
                        subscriber.onError(e);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    subscriber.onError(throwable);
                }

                @Override
                public void onComplete() {
                    if (leftover.get().length > 0) {
                        publisher.submit(List.of(leftover.get())); // Emit last field if necessary
                    }
                    publisher.close();
                    latch.countDown(); // Signal that processing is complete
                    System.out.println("Publisher closed");
                }
            });

            publisher.subscribe(subscriber);

            // Wait for all tasks to complete before shutting down the executor
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                executor.shutdown();
                System.out.println("Executor shut down");
            }
        };
    }

    @Override
    public Flow.Publisher<List<byte[]>> parse(Flow.Publisher<ByteBuffer> byteBufferStream) {
        return parse(byteBufferStream, (byte) ',');
    }
}