package reactive.async.compress.reactive;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;
import java.util.zip.Inflater;

public class ReactiveZipProcessorToSout implements Flow.Subscriber<ByteBuffer> {
    private Flow.Subscription subscription;
    private ByteBuffer buffer = ByteBuffer.allocate(8192);
    private static final int LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(5);
    }

    @Override
    public void onNext(ByteBuffer chunk) {
        processChunk(chunk);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println("Error in ZIP processing: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("ZIP processing complete.");
    }

    private void processChunk(ByteBuffer chunk) {
        ensureCapacity(chunk.remaining());
        buffer.put(chunk);
        buffer.flip();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        while (buffer.remaining() >= 4) {
            buffer.mark();
            int signature = buffer.getInt();
            if (signature != LOCAL_FILE_HEADER_SIGNATURE) {
                buffer.reset();
                break;
            }

            if (buffer.remaining() < 26) {
                buffer.reset();
                break;
            }

            buffer.getShort(); // Version
            buffer.getShort(); // General purpose bit flag
            int compressionMethod = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt(); // Last mod time & date
            buffer.getInt(); // CRC-32
            int compressedSize = buffer.getInt();
            int uncompressedSize = buffer.getInt();
            int fileNameLength = Short.toUnsignedInt(buffer.getShort());
            int extraFieldLength = Short.toUnsignedInt(buffer.getShort());

            if (buffer.remaining() < (fileNameLength + extraFieldLength)) {
                buffer.reset();
                break;
            }

            byte[] fileNameBytes = new byte[fileNameLength];
            buffer.get(fileNameBytes);
            String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

            buffer.position(buffer.position() + extraFieldLength);

            if (buffer.remaining() < compressedSize) {
                buffer.reset();
                break;
            }

            ByteBuffer compressedData = buffer.slice().limit(compressedSize);
            buffer.position(buffer.position() + compressedSize);

            Flux<ByteBuffer> decompressedStream = decompressStream(compressedData, compressionMethod);
            System.out.println("Processing file: " + fileName);
            decompressedStream.subscribe(new ByteBufferSubscriber(fileName));
        }

        buffer.compact();
    }

    private void ensureCapacity(int extraBytes) {
        if (buffer.remaining() < extraBytes) {
            ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
    }

    private Flux<ByteBuffer> decompressStream(ByteBuffer compressedData, int compressionMethod) {
        if (compressionMethod == 0) {
            return Flux.just(compressedData);
        } else {
            return Flux.create(sink -> {
                Inflater inflater = new Inflater(true);
                byte[] input = new byte[compressedData.remaining()];
                compressedData.get(input);
                inflater.setInput(input);
                byte[] outputBuffer = new byte[4096];

                try {
                    while (!inflater.finished() && inflater.getRemaining() > 0) {
                        int decompressedBytes = inflater.inflate(outputBuffer);
                        sink.next(ByteBuffer.wrap(outputBuffer, 0, decompressedBytes));
                    }
                    sink.complete();
                } catch (Exception e) {
                    sink.error(e);
                } finally {
                    inflater.end();
                }
            });
        }
    }

    private static class ByteBufferSubscriber implements Subscriber<ByteBuffer> {
        private final String fileName;

        public ByteBufferSubscriber(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            String output = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("[" + fileName + "] Output chunk: " + output);
        }

        @Override
        public void onError(Throwable throwable) {
            System.err.println("Error processing file: " + fileName + " - " + throwable.getMessage());
        }

        @Override
        public void onComplete() {
            System.out.println("Decompression complete for file: " + fileName);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        java.util.concurrent.SubmissionPublisher<ByteBuffer> publisher = new java.util.concurrent.SubmissionPublisher<>();
        ReactiveZipProcessorToSout parser = new ReactiveZipProcessorToSout();
        publisher.subscribe(parser);

        byte[] zipBytes = loadZipFileAsBytes("D:/cesop/async/001-testdata.zip");
        int chunkSize = 1024;
        for (int i = 0; i < zipBytes.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, zipBytes.length);
            publisher.submit(ByteBuffer.wrap(zipBytes, i, end - i));
        }

        publisher.close();
        Thread.sleep(100000);
    }

    private static byte[] loadZipFileAsBytes(String path) {
        try {
            return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));
        } catch (Exception e) {
            throw new RuntimeException("Error reading ZIP file", e);
        }
    }
}
