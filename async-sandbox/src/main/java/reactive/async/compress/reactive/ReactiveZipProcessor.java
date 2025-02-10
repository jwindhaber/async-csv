package reactive.async.compress.reactive;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Inflater;

public class ReactiveZipProcessor {

    private static final int LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50;

    public static Flux<Flux<ByteBuffer>> processZip(Flux<ByteBuffer> zipChunks) {
        return Flux.create(sink -> {
            AtomicReference<ByteBuffer> bufferRef = new AtomicReference<>(ByteBuffer.allocate(8192));

            zipChunks.subscribe(
                    chunk -> {
                        ByteBuffer currentBuffer = bufferRef.get();
                        bufferRef.set(processChunk(currentBuffer, chunk, sink));
                    },
                    sink::error,
                    sink::complete
            );
        }, FluxSink.OverflowStrategy.BUFFER);
    }

    private static ByteBuffer processChunk(ByteBuffer buffer, ByteBuffer chunk, FluxSink<Flux<ByteBuffer>> sink) {
        buffer = ensureCapacity(buffer, chunk.remaining());
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
            short generalPurposeFlag = buffer.getShort();
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

            // 🚀 **Fix: Handle "streaming mode" ZIPs where sizes are unknown**
            boolean hasDataDescriptor = (generalPurposeFlag & 0x08) != 0;
            if (hasDataDescriptor) {
                System.out.println("Streaming mode ZIP detected for file: " + fileName);
                compressedSize = -1; // We do not know the size upfront
            }

            // If compressedSize is still -1, read until EOF or the next ZIP entry
            ByteBuffer compressedData = extractCompressedData(buffer, compressedSize);
            Flux<ByteBuffer> decompressedStream = decompressStream(compressedData, compressionMethod);

            sink.next(decompressedStream);
        }

        buffer.compact();
        return buffer;
    }

    private static ByteBuffer extractCompressedData(ByteBuffer buffer, int compressedSize) {
        if (compressedSize > 0) {
            // We know the compressed size, so we can safely slice it
            ByteBuffer compressedData = buffer.slice();
            compressedData.limit(compressedSize);
            buffer.position(buffer.position() + compressedSize);
            return compressedData;
        }

        // 🚀 **Handle streaming ZIPs (compressedSize == -1)**
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        while (buffer.remaining() >= 4) {
            buffer.mark();
            int signature = buffer.getInt();

            if (isZipSignature(signature)) {
                buffer.reset();
                break;
            }

            buffer.reset();
            byte b = buffer.get();
            outputStream.write(b);
        }

        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    private static boolean isZipSignature(int signature) {
        return signature == LOCAL_FILE_HEADER_SIGNATURE;
    }


    private static ByteBuffer ensureCapacity(ByteBuffer buffer, int extraBytes) {
        if (buffer.remaining() >= extraBytes) {
            return buffer; // Enough space already
        }

        int newCapacity = Math.max(buffer.capacity() * 2, buffer.position() + extraBytes);
        ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
        buffer.flip();
        newBuffer.put(buffer);
        return newBuffer;
    }

    private static Flux<ByteBuffer> decompressStream(ByteBuffer compressedData, int compressionMethod) {
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

    public static void main(String[] args) throws InterruptedException {
        Flux<ByteBuffer> zipChunks = Flux.just(ByteBuffer.wrap(loadZipFileAsBytes("D:/cesop/async/inserts-50_000_000.zip")));

        System.out.println("Start processing");

        Flux<Flux<ByteBuffer>> decompressedFiles = processZip(zipChunks);

        decompressedFiles.subscribe(fileFlux -> {
            System.out.println("Processing a new file:");
            fileFlux.subscribe(chunk -> {
                byte[] bytes = new byte[chunk.remaining()];
                chunk.get(bytes);
//                System.out.println("Decompressed chunk: " + new String(bytes, StandardCharsets.UTF_8));
                System.out.println("Decompressed chunk size: " + bytes.length);
            });
        });

//        Thread.sleep(10000);
    }

    private static byte[] loadZipFileAsBytes(String path) {
        try {
            return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));
        } catch (Exception e) {
            throw new RuntimeException("Error reading ZIP file", e);
        }
    }
}
