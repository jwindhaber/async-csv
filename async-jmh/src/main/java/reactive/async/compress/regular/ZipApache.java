package reactive.async.compress.regular;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.FileInputStream;
import java.io.IOException;

public class ZipApache {

    public static void main(String[] args) {
        String zipFilePath = "D:\\cesop\\001-testdata\\inserts-1_000.zip";
            try (FileInputStream fis = new FileInputStream(zipFilePath);
                 ZipArchiveInputStream zipStream = new ZipArchiveInputStream(fis)) {

                ZipArchiveEntry entry;
                byte[] buffer = new byte[4096];

                while ((entry = zipStream.getNextEntry()) != null) {
                    System.out.println("Extracting: " + entry.getName());

                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    int bytesRead;
                    while ((bytesRead = zipStream.read(buffer)) != -1) {
                        output.write(buffer, 0, bytesRead);
                    }

                    // Write extracted data to System.out
                    System.out.write(output.toByteArray());
                    System.out.println("\nExtracted " + entry.getName() + " (" + output.size() + " bytes)");
                }
            } catch (Exception e) {
                System.err.println("Error processing ZIP archive: " + e.getMessage());
            }
    }
}
