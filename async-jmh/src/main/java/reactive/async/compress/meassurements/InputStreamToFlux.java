package reactive.async.compress.meassurements;

import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class InputStreamToFlux {
    private static final int BUFFER_SIZE = 8192; // Adjust for performance

    public static Flux<ByteBuffer> fromInputStream(InputStream inputStream) {
        return Flux.generate(
                () -> inputStream,
                (stream, sink) -> {
                    try {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int bytesRead = stream.read(buffer);

                        if (bytesRead == -1) {
                            sink.complete(); // End of stream
                        } else {
                            sink.next(ByteBuffer.wrap(buffer, 0, bytesRead));
                        }
                    } catch (Exception e) {
                        sink.error(e); // Handle errors
                    }
                    return stream;
                },
                stream -> {
                    try {
                        stream.close(); // Cleanup when Flux is terminated
                    } catch (Exception ignored) {}
                }
        );
    }
}
