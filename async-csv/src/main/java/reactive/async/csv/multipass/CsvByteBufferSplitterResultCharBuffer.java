package reactive.async.csv.multipass;

import reactive.async.csv.LeftoverProvider;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import static reactive.async.csv.multipass.IsolatedBatchReorder.combine;

public class CsvByteBufferSplitterResultCharBuffer {//implements LeftoverProvider {


    private final CharBuffer buffer;
    private final CharBuffer leftover;

    public CsvByteBufferSplitterResultCharBuffer(CharBuffer buffer, CharBuffer leftover) {
        this.buffer = buffer;
        this.leftover = leftover;
    }

    public CharBuffer getBuffer() {
        return buffer;
    }

    public static void main(String[] args) {
//        ByteBuffer buffer = ByteBuffer.wrap("line1\nline2\n\"partial\nline\"".getBytes());
//        ByteBuffer buffer = ByteBuffer.wrap("aa\naa,\"ab\n\"".getBytes());
        ByteBuffer buffer = ByteBuffer.wrap("aa\naa,\"ab\n".getBytes());
//        CsvBufferSplitterResult splitBuffers = splitBufferAtLastNewline(buffer,',', ByteBuffer.allocate(0));

//        System.out.println("Rest of the buffer:");
//        while (splitBuffers[0].hasRemaining()) {
//            System.out.print((char) splitBuffers[0].get());
//        }
//
//        System.out.println("\nPartial line:");
//        while (splitBuffers[1].hasRemaining()) {
//            System.out.print((char) splitBuffers[1].get());
//        }
    }

    public static CsvByteBufferSplitterResultCharBuffer splitBufferAtLastNewline(CharBuffer first, byte delimiter1 , CharBuffer leftover) {


        CharBuffer buffer = combine(leftover, first);

        boolean inQuotes = false;
        boolean newLineFound = false;
        boolean quoteFound = false;
        char delimiter = ',';

        int newLinePosition = -1;

        for (int i = buffer.limit() - 1; i >= 0; i--) {
            char b = buffer.get(i);

            if (b == '\n') {
                if(!newLineFound){
                    newLineFound = true;
                    newLinePosition = i;
                }
            } else if(b == delimiter){
                if(buffer.limit() <= i + 1){
                    continue;
                }
                char afterDelimiter = buffer.get(i + 1);
                if(afterDelimiter != '"' && newLineFound){
                    break;
                }
                if(afterDelimiter == '"'){
                    newLineFound = false;
                }
            }


        }

        if (newLinePosition == -1) {
            return new CsvByteBufferSplitterResultCharBuffer(CharBuffer.allocate(0), buffer);
        }

//        ByteBuffer restBuffer = ByteBuffer.allocate(newLinePosition + 1);
        CharBuffer partialLineBuffer = CharBuffer.allocate(buffer.limit() - newLinePosition - 1);



//        for (int i = 0; i <= newLinePosition; i++) {
//            restBuffer.put(buffer.get(i));
//        }

        for (int i = newLinePosition + 1; i < buffer.limit(); i++) {
            partialLineBuffer.put(buffer.get(i));
        }

//        restBuffer.flip();
        partialLineBuffer.flip();


//        buffer.reset();
        buffer.position(0);
        buffer.limit(newLinePosition + 1);

        return new CsvByteBufferSplitterResultCharBuffer(buffer, partialLineBuffer);
    }

    //@Override
    public CharBuffer getLeftover() {
        return leftover;
    }
}