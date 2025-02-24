package reactive.async.csv.zerocopy;

import reactive.async.csv.LeftoverProvider;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ByteBufferCsvParser  implements LeftoverProvider {

    private final ByteBuffer buffer;
    private final List<int[][]> lines;

    private static final int MAX_FIELDS = 17;

    public ByteBufferCsvParser(ByteBuffer buffer, List<int[][]> lines) {
        this.buffer = buffer;
        this.lines = lines;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public List<int[][]> getLines() {
        return lines;
    }

//    public record Row(int start, int end, List<Field> fields){}
//    public record Field(int start, int end){}

    static public class Field {
        int start;
        int end;

        Field(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    static public class Row {
        int lineStart;
        int lineEnd;
        Field[] fields;
        int fieldCount; // Keeps track of the number of fields added

        Row(int lineStart, int lineEnd, Field[] fields, int fieldCount) {
            this.lineStart = lineStart;
            this.lineEnd = lineEnd;
            this.fields = new Field[fieldCount]; // Copy only the needed fields
            System.arraycopy(fields, 0, this.fields, 0, fieldCount);
            this.fieldCount = fieldCount;
        }
    }

//    static class ZeroOverheadResult {
//        ByteBuffer buffer;
//        List<Row> rows;
//
//        ZeroOverheadResult(ByteBuffer buffer, List<Row> rows) {
//            this.buffer = buffer;
//            this.rows = rows;
//        }
//    }

    public static ZeroOverheadResult parseCsv(ByteBuffer buffer) {
        List<Row> rows = new ArrayList<>();
        Field[] fields = new Field[MAX_FIELDS];


        for (int i = 0; i < MAX_FIELDS; i++) {
            fields[i] = new Field(0, 0); // Pre-allocate objects
        }

        int fieldCount = 0;
        int lineStart = buffer.position();
        int fieldStart = lineStart;
        boolean inQuotes = false;
        int limit = buffer.limit();

        for (int position = 0; position < limit; position++) {
            byte b = buffer.get(position);

            if (b == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes && (b == ',' || b == '\n' || b == '\r')) {
                if (fieldCount < MAX_FIELDS) {
                    fields[fieldCount].start = fieldStart;
                    fields[fieldCount].end = buffer.position() - 1;
                    fieldCount++;
                }

                if (b == '\n' || b == '\r') {
                    int lineEnd = buffer.position();
                    if (fieldCount > 0) {
                        rows.add(new Row(lineStart, lineEnd, fields, fieldCount));
                        fieldCount = 0; // Reset for next row
                    }

                    if (b == '\r' && buffer.hasRemaining() && buffer.get(buffer.position()) == '\n') {
                        buffer.get(); // Skip \n
                    }
                    lineStart = buffer.position();
                    fieldStart = lineStart;
                    continue;
                }
                fieldStart = buffer.position();
            }
        }

        // Handle last line if not terminated with a newline
        if (fieldStart < buffer.position() && fieldCount < MAX_FIELDS) {
            fields[fieldCount].start = fieldStart;
            fields[fieldCount].end = buffer.position() - 1;
            fieldCount++;
        }
        if (fieldCount > 0) {
            rows.add(new Row(lineStart, buffer.position(), fields, fieldCount));
        }

        return new ZeroOverheadResult(buffer, rows);
    }

//    private static int[][] toArray(List<int[]> fields, int lineStart, int lineEnd) {
//        int[][] result = new int[fields.size() + 1][2];
//        result[0] = new int[]{lineStart, lineEnd}; // First entry stores line positions
//        for (int i = 0; i < fields.size(); i++) {
//            result[i + 1] = fields.get(i); // Store field positions
//        }
//        return result;
//    }

    public static byte[] extractBytes(ByteBuffer buffer, int start, int end) {
        int length = end - start;
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = buffer.get(start + i);
        }
        return data;
    }

    public static String extractString(ByteBuffer buffer, int start, int end) {
        return new String(extractBytes(buffer, start, end), StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        String sampleCsv = "name,age,city\nJohn,30,\"New York\"\nAlice,25,Boston";
        ByteBuffer buffer = ByteBuffer.wrap(sampleCsv.getBytes());

//        List<Row> parsedCsv = parseCsv(buffer).getLines();

//        for (Row row : parsedCsv) {
//            // Extract and print line
//            int lineStart = row[0][0], lineEnd = row[0][1];
//            System.out.println("Line: [" + lineStart + ":" + lineEnd + "] -> " +
//                    extractString(buffer, lineStart, lineEnd));
//
//            // Extract and print fields
//            for (int i = 1; i < row.length; i++) {
//                int fieldStart = row[i][0], fieldEnd = row[i][1];
//                System.out.println("  Field: [" + fieldStart + ":" + fieldEnd + "] -> " +
//                        extractString(buffer, fieldStart, fieldEnd));
//            }
//        }
    }


    @Override
    public ByteBuffer getLeftover() {
        return null;
    }
}
