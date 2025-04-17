package concurrent.csv;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.StructuredTaskScope;
import java.util.stream.Collectors;

public class StructuredScopeExample {

    public static void main(String[] args) throws Exception {
        List<Integer> input = List.of(1, 2, 3, 4, 5);
        processWithSubtaskQueue(input);

        Thread.sleep(1000);
    }

    public static void processWithSubtaskQueue(List<Integer> numbers) throws Exception {
        BlockingQueue<StructuredTaskScope.Subtask<Integer>> queue = new LinkedBlockingQueue<>();
        ExecutorService consumerThread = Executors.newSingleThreadExecutor();

        // Start consumer thread (runs indefinitely in a server process)
        consumerThread.submit(() -> {
            try {
                while (true) {
                    StructuredTaskScope.Subtask<Integer> subtask = queue.take();

                    try {
                        Integer result = subtask.get(); // blocks until result available
                        System.out.println("Got result: " + result);
                    } catch (Exception e) {
                        System.err.println("Subtask failed: " + e.getMessage());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Consumer interrupted, shutting down.");
            }
        });

        // This simulates one batch of processing
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            for (Integer number : numbers) {
                var subtask = scope.fork(() -> {
//                    if (number == 3) {
//                        throw new RuntimeException("Boom on 3");
//                    }
                    return number * number;
                });
                queue.put(subtask); // immediately put the subtask in the queue
            }

            scope.join().throwIfFailed();
        }
    }
}