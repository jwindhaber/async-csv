package reactive.async.csv;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;

public interface ReactiveCsvParser {

    /**
     * Parses a stream of ByteBuffers into a stream of CSV rows.
     * @param byteBufferStream Flow.Publisher of ByteBuffer (raw CSV data)
     * @param delimiter CSV delimiter (e.g., ',', '|', '\t')
     * @return Flow.Publisher emitting parsed CSV rows as lists of byte arrays
     */
    Flow.Publisher<List<byte[]>> parse(Flow.Publisher<ByteBuffer> byteBufferStream, byte delimiter);

    Flow.Publisher<List<byte[]>> parse(Flow.Publisher<ByteBuffer> byteBufferStream);
}
