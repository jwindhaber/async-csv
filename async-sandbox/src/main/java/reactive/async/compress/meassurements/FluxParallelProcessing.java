package reactive.async.compress.meassurements;

import com.google.common.base.Stopwatch;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;

public class FluxParallelProcessing {




    public static void main(String[] args) throws InterruptedException {
                Stopwatch watch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(1);

        Flux.range(1, 10)
                .flatMapSequential(i ->
                        Flux.just(i)
                                .publishOn(Schedulers.boundedElastic())
                                .map(num -> {
                                    System.out.println("Processing: " + num + " on thread " + Thread.currentThread().getName());
                                    doSomeWork(num);
                                    return num;
                                })
                )
                .doOnTerminate(latch::countDown)
                .subscribe(i -> System.out.println("Result: " + i + " on thread " + Thread.currentThread().getName()));
        latch.await();
        watch.stop();
        System.out.println("Extraction took: " + watch);
    }



//    public static void main(String[] args) throws InterruptedException {
//        Stopwatch watch = Stopwatch.createStarted();
//        CountDownLatch latch = new CountDownLatch(1);
//        Flux.range(1, 10)
////                .publishOn(Schedulers.newSingle("1"))
//                .parallel()
//                .runOn(Schedulers.boundedElastic())
//                .map(i -> {
//                    System.out.println("Step 1: " + i + " on thread " + Thread.currentThread().getName());
//                    doSomeWork(i);
//                    return i;
//                })
//                .sequential()
////                .parallel()
////                .runOn(Schedulers.boundedElastic())
////                .publishOn(Schedulers.newSingle("2"))
////                .publishOn(Schedulers.parallel())
//                .map(i -> {
//                    System.out.println("Step 2: " + i + " on thread " + Thread.currentThread().getName());
//                    doSomeWork(i);
//                    return i;
//                })
////                .sequential()
////                .publishOn(Schedulers.newSingle("3"))
////                .map(i -> {
////                    System.out.println("Step 3: " + i + " on thread " + Thread.currentThread().getName());
////                    doSomeWork(i);
////                    return i;
////                })
////                .publishOn(Schedulers.newSingle("4"))
////                .map(i -> {
////                    System.out.println("Step 4: " + i + " on thread " + Thread.currentThread().getName());
////                    doSomeWork(i);
////                    return i;
////                })
//                .doOnTerminate(latch::countDown)
//                .subscribe(i -> System.out.println("Result: " + i + " on thread " + Thread.currentThread().getName()));
//        latch.await();
//        watch.stop();
//        System.out.println("Extraction took: " + watch);
//    }

    private static void doSomeWork(int number) {
        double sum = 0;
        for (int i = 1; i <= 500000000; i++) {
            sum += Math.sqrt(number + i);
        }
        System.out.println("Sum: " + sum);
    }
}