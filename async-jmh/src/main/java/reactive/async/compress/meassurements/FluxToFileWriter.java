package reactive.async.compress.meassurements;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class FluxToFileWriter {

    public static Mono<Void> writeToFile(Flux<ByteBuffer> byteBufferFlux, Path filePath) {
        try {
            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(
                    filePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE
            );

            AtomicLong position = new AtomicLong(0);

            return byteBufferFlux
                    .concatMap(byteBuffer -> {
                        long pos = position.getAndAdd(byteBuffer.remaining());
                        return writeChunk(fileChannel, byteBuffer, pos);
                    })
                    .then()  // Ensures completion before closing
                    .doFinally(signal -> {
                        try {
                            fileChannel.close();
                        } catch (Exception ignored) {}
                    });
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private static Mono<Void> writeChunk(AsynchronousFileChannel fileChannel, ByteBuffer buffer, long position) {
        return Mono.create(sink -> {
            fileChannel.write(buffer, position, buffer, new java.nio.channels.CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    sink.success();
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    sink.error(exc);
                }
            });
        });
    }
}
