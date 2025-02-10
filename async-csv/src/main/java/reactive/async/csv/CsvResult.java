package reactive.async.csv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CsvResult implements LeftoverProvider {
    private final List<List<byte[]>> lines;
    private final byte[] leftover;

    public CsvResult(List<List<byte[]>> lines, byte[] leftover) {
        this.lines = lines;
        this.leftover = leftover;
    }

    public List<List<byte[]>> getLines() {
        return lines;
    }

    @Override
    public byte[] getLeftover() {
        return this.leftover;
    }

    public static CsvResult fromByteBuffer(ByteBuffer buffer, byte delimiter, byte[] leftover) {
        List<List<byte[]>> lines = new ArrayList<>();
        List<byte[]> currentLine = new ArrayList<>();
        boolean inQuotes = false;
        byte[] newLeftoverBytes = new byte[0];

        try (ByteArrayOutputStream fieldBuffer = new ByteArrayOutputStream()) {

            for (byte b : leftover) {
                if (b == '"') {
                    inQuotes = !inQuotes;
                } else if (!inQuotes) {
                    if (b == delimiter) {

                        currentLine.add(fieldBuffer.toByteArray());
                        fieldBuffer.reset();

                    } else if (b == '\n' || b == '\r') {
                        currentLine.add(fieldBuffer.toByteArray());
                        fieldBuffer.reset();
                        lines.add(currentLine);
                        currentLine = new ArrayList<>();
                    } else {
                        fieldBuffer.write(b);
                    }
                } else {
                    fieldBuffer.write(b);
                }
            }

            int lineStart = 0;

            byte[] array = buffer.array();

//            while (buffer.hasRemaining()) {

            int position = 0;
            for (byte b : array) {

//            for (int i = 0; i < array.length; i++) {
//                byte b = array[i];

//                byte b = buffer.get();
                if (b == '"') {
                    inQuotes = !inQuotes;
                } else if (!inQuotes) {
                    if (b == delimiter) {

                        currentLine.add(fieldBuffer.toByteArray());
                        fieldBuffer.reset();

                    } else if (b == '\n' || b == '\r') {

                        currentLine.add(fieldBuffer.toByteArray());
                        fieldBuffer.reset();
                        lines.add(currentLine);
                        currentLine = new ArrayList<>();

                        lineStart = position;

                    } else {
                        fieldBuffer.write(b);
                    }
                }
                position++;
            }


            if (lineStart < buffer.position()) {
                int fieldLength = buffer.position() - lineStart;
                newLeftoverBytes = new byte[fieldLength];
                buffer.get(lineStart, newLeftoverBytes, 0, fieldLength);
            }
//            String leftoverString = new String(newLeftoverBytes, StandardCharsets.UTF_8);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CsvResult(lines, newLeftoverBytes);

    }

    @Override
    public String toString() {
        return "CsvResult{lines=" + lines + '}';
    }
}