package reactive.async.compress;

import reactor.core.publisher.Flux;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteBufferProvider {

    public static ByteBuffer[] receiveZipChunksArray() {
        Path filePath = Paths.get("D:\\cesop\\001-testdata\\inserts-1_000.zip");
        List<ByteBuffer> chunks = new ArrayList<>();
        byte[] buffer = new byte[4096];
        int bytesRead;

        try (InputStream fis = new FileInputStream(filePath.toFile())) {
            while ((bytesRead = fis.read(buffer)) != -1) {
                chunks.add(ByteBuffer.wrap(Arrays.copyOf(buffer, bytesRead)));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return chunks.toArray(new ByteBuffer[0]);

    }

    public static Flux<ByteBuffer> receiveZipChunksFlux() {
        return Flux.fromArray(receiveZipChunksArray());

    }
}
