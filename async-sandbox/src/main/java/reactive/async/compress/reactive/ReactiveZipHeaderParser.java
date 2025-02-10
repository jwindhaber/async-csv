package reactive.async.compress.reactive;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

public class ReactiveZipHeaderParser implements Flow.Subscriber<ByteBuffer> {
    private Flow.Subscription subscription;
    private ByteBuffer buffer = ByteBuffer.allocate(8192); // Buffer for partial headers
    private static final int LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1); // Request the first chunk
    }

    @Override
    public void onNext(ByteBuffer chunk) {
        processChunk(chunk);
        subscription.request(1); // Request next chunk
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println("Error in ZIP parsing: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("ZIP header parsing complete.");
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

            buffer.getShort(); // Version needed to extract
            buffer.getShort(); // General purpose bit flag
            int compressionMethod = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt(); // Last mod file time & date
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

            System.out.println("File: " + fileName);
            System.out.println("Compression: " + (compressionMethod == 0 ? "Stored" : "Deflated"));
            System.out.println("Compressed Size: " + compressedSize);
            System.out.println("Uncompressed Size: " + uncompressedSize);
            System.out.println("--------------------------------");

            if (buffer.remaining() < compressedSize) {
                buffer.reset();
                break;
            }

            buffer.position(buffer.position() + compressedSize);
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

    public static void main(String[] args) throws InterruptedException {
        SubmissionPublisher<ByteBuffer> publisher = new SubmissionPublisher<>();
        ReactiveZipHeaderParser parser = new ReactiveZipHeaderParser();
        publisher.subscribe(parser);

//        byte[] zipBytes = loadZipFileAsBytes("D:\\cesop\\001-testdata\\inserts-1_000.zip");
        byte[] zipBytes = loadZipFileAsBytes("D:\\cesop\\001-testdata\\001-testdata.zip");
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
