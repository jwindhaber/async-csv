package reactive.async.compress;

import org.apache.commons.compress.archivers.zip.*;
import java.io.*;
import java.nio.ByteBuffer;

public class ZipChunkProcessorApache {
    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // Simulating received ZIP chunks
        byte[][] zipChunks = receiveZipChunks();

        for (byte[] chunk : zipChunks) {
            buffer.write(chunk); // Accumulate ZIP data
            processZipEntries(new ByteArrayInputStream(buffer.toByteArray())); // Try extracting files
        }
    }

    private static void processZipEntries(InputStream inputStream) {
        try (ZipArchiveInputStream zipStream = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            byte[] buffer = new byte[4096];

            while ((entry = zipStream.getNextEntry()) != null) {
                System.out.println("Extracting: " + entry.getName());

                ByteArrayOutputStream output = new ByteArrayOutputStream();
                int bytesRead;
                while ((bytesRead = zipStream.read(buffer)) != -1) {
                    output.write(buffer, 0, bytesRead);
                }

                // Do something with extracted data
                System.out.println("Extracted " + entry.getName() + " (" + output.size() + " bytes)");
            }
        } catch (IOException e) {
            System.err.println("Error processing ZIP archive: " + e.getMessage());
        }
    }

    private static byte[][] receiveZipChunks() {
        ByteBufferProvider.receiveZipChunksArray();
        ByteBuffer[] byteBuffers = ByteBufferProvider.receiveZipChunksArray();
        byte[][] byteArrays = new byte[byteBuffers.length][];

        for (int i = 0; i < byteBuffers.length; i++) {
            byteBuffers[i].rewind();
            byteArrays[i] = new byte[byteBuffers[i].remaining()];
            byteBuffers[i].get(byteArrays[i]);
        }
        return byteArrays;
    }
}
