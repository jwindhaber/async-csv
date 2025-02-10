package reactive.async.csv;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;


public class ReactiveUtils {

    public static <T> Publisher<T> toPublisher(Flux<T> flux) {
        return flux;
    }
}
