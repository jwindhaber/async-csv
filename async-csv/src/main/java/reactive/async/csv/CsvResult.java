package reactive.async.csv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CsvResult implements LeftoverProvider {
    public static final byte QUOTE = (byte) '"';
    private final List<List<byte[]>> lines;
    private final ByteBuffer leftover;

    public CsvResult(List<List<byte[]>> lines, ByteBuffer leftover) {
        this.lines = lines;
        this.leftover = leftover;
    }

    public List<List<byte[]>> getLines() {
        return lines;
    }

    @Override
    public ByteBuffer getLeftover() {
        return this.leftover;
    }

    public static CsvResult fromByteBuffer(ByteBuffer buffer, byte delimiter, ByteBuffer leftover) {
        List<List<byte[]>> lines = new ArrayList<>();
        List<byte[]> currentLine = new ArrayList<>();
        boolean inQuotes = false;
        byte[] newLeftoverBytes = new byte[0];

        int lineStart = 0;
        ByteBuffer combined = combine(leftover, buffer);
        int limit = combined.limit();
        ByteBuffer fieldBuffer = ByteBuffer.allocate(combined.capacity());

        for (int position = 0; position < limit; position++) {
            byte b = combined.get();
            if (b == QUOTE) {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (b == delimiter) {
                    byte[] field = new byte[fieldBuffer.position()];
                    fieldBuffer.flip();
                    fieldBuffer.get(field);
                    currentLine.add(field);
                    fieldBuffer.clear();
                } else if (b == '\n' || b == '\r') {
                    byte[] field = new byte[fieldBuffer.position()];
                    fieldBuffer.flip();
                    fieldBuffer.get(field);
                    currentLine.add(field);
                    fieldBuffer.clear();
                    lines.add(currentLine);
                    currentLine = new ArrayList<>();
                    lineStart = position + 1;
                } else {
                    fieldBuffer.put(b);
                }
            } else {
                fieldBuffer.put(b);
            }
        }


        if (lineStart < combined.position()) {
            int fieldLength = combined.position() - lineStart;
            newLeftoverBytes = new byte[fieldLength];
            combined.get(lineStart, newLeftoverBytes, 0, fieldLength);
        }
//            String leftoverString = new String(newLeftoverBytes, StandardCharsets.UTF_8);
        if(newLeftoverBytes.length > 0) {
            System.out.println("Leftover: " + new String(newLeftoverBytes));
        }

        return new CsvResult(lines, ByteBuffer.wrap(newLeftoverBytes));

    }

    @Override
    public String toString() {
        return "CsvResult{lines=" + lines + '}';
    }


    public static ByteBuffer combine(ByteBuffer buffer1, ByteBuffer buffer2) {
        // Create a new ByteBuffer with a capacity equal to the sum of the two buffers' remaining capacities
        ByteBuffer combinedBuffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
        combinedBuffer.put(buffer1);
        combinedBuffer.put(buffer2);
        combinedBuffer.flip();

        return combinedBuffer;
    }
}