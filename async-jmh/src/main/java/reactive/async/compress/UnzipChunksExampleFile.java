package reactive.async.compress;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnzipChunksExampleFile {
    private static final int BUFFER_SIZE = 1024; // Default buffer size
                
    public static void main(String[] args) throws IOException {
        Path filePath = Paths.get("D:\\cesop\\001-testdata\\inserts-1_000.zip");

        try (InputStream fis = new FileInputStream(filePath.toFile());
             ZipInputStream zis = new ZipInputStream(fis)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                System.out.println("Extracting: " + entry.getName());
                List<ByteBuffer> compressedChunks = readEntryToByteBuffers(zis);
                byte[] decompressedData = decompressChunks(compressedChunks);
                System.out.println(new String(decompressedData));
                zis.closeEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<ByteBuffer> readEntryToByteBuffers(ZipInputStream zis) throws IOException {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        byte[] buffer = new byte[BUFFER_SIZE];
        int len;
        while ((len = zis.read(buffer)) > 0) {
            byteBuffers.add(ByteBuffer.wrap(buffer, 0, len));
        }
        return byteBuffers;
    }

    public static byte[] decompressChunks(List<ByteBuffer> compressedChunks) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        for (ByteBuffer chunk : compressedChunks) {
            byte[] chunkArray = new byte[chunk.remaining()];
            chunk.get(chunkArray);
            outputStream.write(chunkArray);
        }

        return outputStream.toByteArray();
    }
}