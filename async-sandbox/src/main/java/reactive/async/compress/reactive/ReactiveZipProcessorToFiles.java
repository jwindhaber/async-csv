package reactive.async.compress.reactive;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.zip.Inflater;

public class ReactiveZipProcessorToFiles implements Flow.Subscriber<ByteBuffer> {
    private Flow.Subscription subscription;
    private ByteBuffer buffer = ByteBuffer.allocate(8192);
    private static final int LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(5); // Request multiple chunks for better throughput
    }

    @Override
    public void onNext(ByteBuffer chunk) {
        processChunk(chunk);
        subscription.request(1); // Request more chunks as needed
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

            System.out.println("Extracting: " + fileName);
            System.out.println("Compression: " + (compressionMethod == 0 ? "Stored" : "Deflated"));

            byte[] compressedData = new byte[compressedSize];
            if (buffer.remaining() < compressedSize) {
                buffer.reset();
                break;
            }

            buffer.get(compressedData);
            decompressAndWriteFile(fileName, compressedData, uncompressedSize, compressionMethod);
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

    private void decompressAndWriteFile(String fileName, byte[] compressedData, int uncompressedSize, int compressionMethod) {
        CompletableFuture.runAsync(() -> {
            try {
                byte[] outputData;
                if (compressionMethod == 0) {
                    outputData = compressedData;
                } else {
                    outputData = decompress(compressedData, uncompressedSize);
                }

                try (FileOutputStream fos = new FileOutputStream("D:/cesop/async/" + fileName)) {
                    fos.write(outputData);
                }
                System.out.println("Saved: " + fileName);
            } catch (IOException e) {
                System.err.println("Error writing file: " + fileName + " - " + e.getMessage());
            }
        });
    }

    private byte[] decompress(byte[] compressedData, int uncompressedSize) throws IOException {
        Inflater inflater = new Inflater(true);
        inflater.setInput(compressedData);
        byte[] result = new byte[uncompressedSize];
        try {
            inflater.inflate(result);
        } catch (Exception e) {
            throw new IOException("Decompression failed", e);
        } finally {
            inflater.end();
        }
        return result;
    }

    public static void main(String[] args) throws InterruptedException {
        SubmissionPublisher<ByteBuffer> publisher = new SubmissionPublisher<>();
        ReactiveZipProcessorToFiles parser = new ReactiveZipProcessorToFiles();
        publisher.subscribe(parser);

        byte[] zipBytes = loadZipFileAsBytes("D:/cesop/async/001-testdata.zip");
        int chunkSize = 1024;
        for (int i = 0; i < zipBytes.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, zipBytes.length);
            publisher.submit(ByteBuffer.wrap(zipBytes, i, end - i));
        }

        publisher.close();
        Thread.sleep(100000); // Wait for processing to complete
    }

    private static byte[] loadZipFileAsBytes(String path) {
        try {
            return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));
        } catch (Exception e) {
            throw new RuntimeException("Error reading ZIP file", e);
        }
    }
}
