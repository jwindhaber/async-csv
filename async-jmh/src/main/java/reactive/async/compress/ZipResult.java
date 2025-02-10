package reactive.async.compress;

import reactive.async.csv.LeftoverProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class ZipResult implements LeftoverProvider {
    private final List<List<byte[]>> lines;
    private final byte[] leftover;

    public ZipResult(List<List<byte[]>> lines, byte[] leftover) {
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

    public static ZipResult fromByteBuffer(ByteBuffer buffer, byte delimiter, byte[] leftover) {
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

            while (buffer.hasRemaining()) {
                byte b = buffer.get();
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

                        lineStart = buffer.position();

                    } else {
                        fieldBuffer.write(b);
                    }
                }
            }


            if (lineStart < buffer.position()) {
                int fieldLength = buffer.position() - lineStart;
                newLeftoverBytes = new byte[fieldLength];
                buffer.get(lineStart, newLeftoverBytes, 0, fieldLength);
            }
            String leftoverString = new String(newLeftoverBytes, StandardCharsets.UTF_8);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new ZipResult(lines, newLeftoverBytes);

    }

    @Override
    public String toString() {
        return "CsvResult{lines=" + lines + '}';
    }
}