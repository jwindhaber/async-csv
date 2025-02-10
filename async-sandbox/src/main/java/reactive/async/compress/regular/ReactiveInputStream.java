package reactive.async.compress.regular;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ReactiveInputStream extends InputStream {

    private final BlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<>(1);
    private ByteBuffer currentBuffer;
    private boolean completed = false;

    public ReactiveInputStream(Publisher<ByteBuffer> publisher) {
        Flux.from(publisher)
            .publishOn(Schedulers.boundedElastic())
            .doOnNext(buffer -> {
                try {
                    queue.put(buffer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })
            .doOnComplete(() -> completed = true)
            .subscribe();
    }

    @Override
    public int read() throws IOException {
        if (currentBuffer == null || !currentBuffer.hasRemaining()) {
            if (!fillBuffer()) {
                return -1; // End of stream
            }
        }
        return currentBuffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (currentBuffer == null || !currentBuffer.hasRemaining()) {
            if (!fillBuffer()) {
                return -1; // End of stream
            }
        }
        int bytesRead = Math.min(len, currentBuffer.remaining());
        currentBuffer.get(b, off, bytesRead);
        return bytesRead;
    }

    private boolean fillBuffer() throws IOException {
        try {
            currentBuffer = queue.take();
            return currentBuffer != null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for data", e);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        completed = true;
    }
}