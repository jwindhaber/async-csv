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

    public record Row(int start, int end, List<Field> fields){}
    public record Field(int start, int end){}

    public static ZeroOverheadResult parseCsv(ByteBuffer buffer) {
        //we could init with the number of columns from the header
        List<Row> rows = new ArrayList<>();
        //Lets assume we have
        List<Field> fields = new ArrayList<>();

        int lineStart = buffer.position();
        int fieldStart = lineStart;
        boolean inQuotes = false;
        int limit = buffer.limit();
        for (int position = 0; position < limit; position++) {
            byte b = buffer.get();

            if (b == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes && (b == ',' || b == '\n' || b == '\r')) {
                Field field = new Field(fieldStart, buffer.position() - 1);
                fields.add(field); // Store field positions
//                fields.add(new int[]{fieldStart, buffer.position() - 1}); // Store field positions

                if (b == '\n' || b == '\r') {
                    int lineEnd = buffer.position();
                    if (!fields.isEmpty()) {

                        rows.add(new Row(lineStart, lineEnd, fields));
//                        rows.add(toArray(fields, lineStart, lineEnd));
                        fields.clear();
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
        if (fieldStart < buffer.position()) {
            fields.add(new Field(fieldStart, buffer.position() - 1));
        }
        if (!fields.isEmpty()) {
            rows.add(new Row(lineStart, buffer.position(), fields));
//            rows.add(toArray(fields, lineStart, buffer.position()));
        }

        return new ZeroOverheadResult(buffer, rows);
//        return new ByteBufferCsvParser(buffer, rows);
//        return rows;
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

        List<Row> parsedCsv = parseCsv(buffer).getLines();

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
