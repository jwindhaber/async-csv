package reactive.async.csv.multipass;

import reactive.async.csv.LeftoverProvider;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class IsolatedBatchReorder implements LeftoverProvider {
    public static final byte QUOTE = (byte) '"';
    private final ByteBuffer buffer;
    private final ByteBuffer leftover;

    public IsolatedBatchReorder(ByteBuffer buffer, ByteBuffer leftover) {
        this.buffer = buffer;
        this.leftover = leftover;
    }



    @Override
    public ByteBuffer getLeftover() {
        return this.leftover;
    }

    public static IsolatedBatchReorder fromByteBuffer(ByteBuffer buffer, byte delimiter, ByteBuffer leftover) {
        ByteBuffer combined = combine(buffer, leftover);



        return null;
    }

    @Override
    public String toString() {
        return "";
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