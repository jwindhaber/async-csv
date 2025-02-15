package reactive.async.compress.meassurements;

import com.google.common.base.Stopwatch;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.reactivestreams.Publisher;
import reactive.async.csv.CsvResult;
import reactive.async.csv.EnhancedByteBufferProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static reactive.async.compress.meassurements.InputStreamToFlux.fromInputStream;

public class ZipApacheEfficient {

    private static final int BUFFER_SIZE = 8192;

    private static final Scheduler inputStreamScheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor());
    private static final Scheduler processingScheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor());




    public static void main(String[] args) throws InterruptedException {
        String zipFilePath = "D:/cesop/async/001-testdata.zip";
        File zipFile = new File(zipFilePath);
        boolean goForFlux = true;



        EnhancedByteBufferProcessor<CsvResult> processor = new EnhancedByteBufferProcessor<>(
                (buffer, leftover) -> CsvResult.fromByteBuffer(buffer, (byte) ',', leftover), // Use CsvResult::fromByteBuffer with leftover
                EnhancedByteBufferProcessor.ErrorHandlingStrategy.SKIP_ON_ERROR,
                () -> new CsvResult(List.of(List.of("Fallback".getBytes())), ByteBuffer.allocate(0)), // Provide a fallback CsvResult
                (byte) ',' // CSV delimiter
        );
        CountDownLatch latch = new CountDownLatch(1);

        Stopwatch watch = Stopwatch.createStarted();
        System.out.println("Extracting ZIP file: " + zipFile.getAbsolutePath());
        try (ZipFile zip = new ZipFile(zipFile, StandardCharsets.UTF_8.name())) {

            List<ZipArchiveEntry> entryList = Collections.list(zip.getEntries());
            entryList
                    .parallelStream()
                    .forEach(entry -> {
                        System.out.println("Extracting: " + entry.getName());

                        AtomicInteger counter = new AtomicInteger(0);

                        if(goForFlux){
                            try {

                                Path outputFile = Path.of("D:/cesop/async/extracted/" + entry.getName());
                                Flux<ByteBuffer> byteBufferFlux = fromInputStream(zip.getInputStream(entry));
                                Flux<ByteBuffer> byteBufferFlux1 = byteBufferFlux.publishOn(processingScheduler);


                                Publisher<CsvResult> resultPublisher = processor.process(byteBufferFlux1);
                                Flux.from(resultPublisher)
                                        .flatMap(csvResult -> Flux.fromIterable(csvResult.getLines()))
                                        .map(line -> line.add(ByteBuffer.allocate(4).putInt(counter.getAndIncrement()).array()))
                                        .count()
                                        .doOnTerminate(latch::countDown)
                                        .subscribe(System.out::println);


                                latch.await();
//                                        .doOnTerminate(() -> System.out.println("Writing complete!"))
//                                        .block();
//
//
//                                FluxToFileWriter.writeToFile(byteBufferFlux, outputFile)
//                                        .doOnTerminate(() -> System.out.println("Writing complete!"))
//                                        .block(); // This ensures the program waits

                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }

                        } else {


//                            File outputFile = new File("D:/cesop/async/extracted/" + entry.getName());

                            Path outputFile = Path.of("D:/cesop/async/extracted/" + entry.getName());

                            try (InputStream inputStream = zip.getInputStream(entry);
                                 FileOutputStream fos = new FileOutputStream(outputFile.toFile())) {

                                byte[] buffer = new byte[BUFFER_SIZE];
                                int bytesRead;
                                while ((bytesRead = inputStream.read(buffer)) != -1) {
                                    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
                                    // Intercept the ByteBuffer here
                                    fos.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
                                }

                                System.out.println("Extracted " + entry.getName() + " (" + Files.size(outputFile) + " bytes)");
                            } catch (IOException e) {
                                System.err.println("Error extracting " + entry.getName() + ": " + e.getMessage());
                            }




//                            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
//                                IOUtils.copy(zip.getInputStream(entry), fos);
//                                System.out.println("Extracted " + entry.getName() + " (" + outputFile.length() + " bytes)");
//                            } catch (IOException e) {
//                                System.err.println("Error extracting " + entry.getName() + ": " + e.getMessage());
//                            }
                        }

                    });


        } catch (IOException e) {
            System.err.println("Error processing ZIP archive: " + e.getMessage());
        }


        watch.stop();
        System.out.println("Extraction took: " + watch);


    }
}