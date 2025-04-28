package concurrent.csv.queue;

import java.nio.ByteBuffer;

public class ChunkedFileProcessorHelper {

    public static int findLastCompleteCsvRecord(ByteBuffer buffer) {
        boolean inQuotes = false;
        for (int i = buffer.limit() - 1; i >= buffer.position(); i--) {
            byte b = buffer.get(i);
            if (b == '"') inQuotes = !inQuotes;
            if (b == '\n' && !inQuotes) return i + 1;
        }
        return -1;
    }
}
