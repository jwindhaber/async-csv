// EnhancedByteBufferProcessor.java
package reactive.async.csv;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class EnhancedByteBufferProcessor<T extends LeftoverProvider> {

    public enum ErrorHandlingStrategy {
        FAIL_FAST,     // Propagate error immediately
        SKIP_ON_ERROR, // Skip problematic ByteBuffers
        FALLBACK       // Replace with fallback value
    }

    private final BiFunction<ByteBuffer, ByteBuffer, T> transformer;
    private final ErrorHandlingStrategy errorHandlingStrategy;
    private final Supplier<T> fallbackSupplier;  // Used for FALLBACK strategy
    private final byte delimiter;
    private ByteBuffer leftover = ByteBuffer.allocate(0);;

    public EnhancedByteBufferProcessor(BiFunction<ByteBuffer, ByteBuffer, T> transformer,
                                       ErrorHandlingStrategy errorHandlingStrategy,
                                       Supplier<T> fallbackSupplier,
                                       byte delimiter) {
        this.transformer = transformer;
        this.errorHandlingStrategy = errorHandlingStrategy;
        this.fallbackSupplier = fallbackSupplier;
        this.delimiter = delimiter;
    }

    public Publisher<T> process(Publisher<ByteBuffer> byteBufferPublisher) {
        return new ByteBufferPublisher(byteBufferPublisher);
    }

    private class ByteBufferPublisher implements Publisher<T> {
        private final Publisher<ByteBuffer> byteBufferPublisher;

        ByteBufferPublisher(Publisher<ByteBuffer> byteBufferPublisher) {
            this.byteBufferPublisher = byteBufferPublisher;
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            byteBufferPublisher.subscribe(new Subscriber<ByteBuffer>() {
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription subscription) {
                    this.subscription = subscription;
                    subscriber.onSubscribe(subscription);
                }

                @Override
                public void onNext(ByteBuffer buffer) {
                    try {

                        T result = transformer.apply(buffer, leftover);
                        leftover = result.getLeftover();
                        subscriber.onNext(result);
                    } catch (Exception e) {
                        handleProcessingError(subscriber, e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    subscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }

                private void handleProcessingError(Subscriber<? super T> subscriber, Exception e) {
                    switch (errorHandlingStrategy) {
                        case FAIL_FAST:
                            subscriber.onError(e);
                            break;
                        case SKIP_ON_ERROR:
                            // Log and continue
                            System.err.println("Skipping corrupted ByteBuffer: " + e.getMessage());
                            break;
                        case FALLBACK:
                            T fallbackValue = fallbackSupplier.get();
                            if (fallbackValue != null) {
                                subscriber.onNext(fallbackValue);
                            }
                            break;
                    }
                }
            });
        }
    }
}