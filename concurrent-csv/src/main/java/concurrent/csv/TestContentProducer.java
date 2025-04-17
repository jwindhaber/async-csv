package concurrent.csv;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestContentProducer {


    public static void main(String[] args) throws IOException {
       var filePath = new File("D:/tmp/sandbox/input-1.txt"); // Specify the file path
        if (!filePath.exists()) {
            filePath.getParentFile().mkdirs(); // Create parent directories if they don't exist
            filePath.createNewFile(); // Create the file if it doesn't exist
        } // Specify the file path
        int start = 1; // Starting number
        int end = 1_000; // Ending number

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (int i = start; i <= end; i++) {
                writer.write(String.valueOf(i));
                writer.newLine(); // Write each number on a new line
            }
            System.out.println("File written successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
