package reactive.async.csv;

import java.nio.ByteBuffer;

public interface LeftoverProvider {
    ByteBuffer getLeftover();
}
