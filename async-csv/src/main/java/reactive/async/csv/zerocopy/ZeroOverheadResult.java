package reactive.async.csv.zerocopy;

import java.nio.ByteBuffer;
import java.util.List;

public class ZeroOverheadResult {

    private ByteBuffer buffer;
    private List<ByteBufferCsvParser.Row> lines;

    public ZeroOverheadResult(ByteBuffer buffer, List<ByteBufferCsvParser.Row> lines) {
        this.buffer = buffer;
        this.lines = lines;
    }

    public List<ByteBufferCsvParser.Row> getLines() {
        return lines;
    }
}
