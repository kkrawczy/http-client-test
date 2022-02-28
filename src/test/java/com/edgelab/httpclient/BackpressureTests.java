package com.edgelab.httpclient;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;
import static java.util.stream.Collectors.toList;

@Slf4j
class BackpressureTests {
    public static final int CLIENTS_NO = 1000000;
    public static final int RECCO2_CALLS_PER_CLIENT = 10;

    Recco2DataRepository recco2DataRepository = new Recco2DataRepository();

    /*
    Run and observe how many http request are launched with pool with only 2 threads.
    Observe java.io.IOException: too many concurrent streams
     */
    @Test
    void with_unlimited_forkJoinPool_current_setup() {
        int parallelism = 2;
        ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism);
        Stream<Integer> clientsToProcess = IntStream.range(1, CLIENTS_NO).boxed();

        log.info("start clients processing batch");

        ForkJoinTask<?> submit = forkJoinPool.submit(() -> clientsToProcess
            .parallel().forEach(this::prepareReport)
        );
        System.out.println(forkJoinPool.getQueuedTaskCount());
        submit.quietlyJoin();

        log.info("finished clients processing batch");
    }

    @Test
    /*
    Only two threads are used.
     */
    void with_limited_ForkJoinPool() {
        int parallelism = 2;
        ForkJoinPool forkJoinPool = new ForkJoinPool
            (parallelism, defaultForkJoinWorkerThreadFactory, null, true,
             0, parallelism, 1, null, 60_000L, TimeUnit.MILLISECONDS);
        Stream<Integer> clientsToProcess = IntStream.range(1, CLIENTS_NO).boxed();

        log.info("start clients processing batch");

        forkJoinPool.submit(() -> clientsToProcess
            .parallel().forEach(this::prepareReport)
        ).quietlyJoin();

        log.info("finished clients processing batch");
    }

    @Test
    void with_custom_backpressure() {
        int concurrency = 4;
        final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(10);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS, queue);

        Stream<Integer> clientsToProcess = IntStream.range(1, CLIENTS_NO).boxed();

        List<Future<String>> clientsProcessingFutures = clientsToProcess.map(clientNo -> {
            while (executorService.getQueue().remainingCapacity() == 0) {
                //we add a new task for execution only if the queue is not full
                log.info("waiting, the queue is full");
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {}
            }
            return executorService.submit(() -> {
                prepareReport(clientNo);
                return "done";
            });
        }).collect(Collectors.toList());

        clientsProcessingFutures.forEach(it -> {
            try {
                it.get();
            } catch (Exception ignored) {}
        });

        log.info("we are done with this part of the batch, can continue");
    }

    static AtomicInteger recco2CallsCounter = new AtomicInteger(1);

    private void prepareReport(Integer clientNo) {
        log.info("start processing client: " + clientNo);

        List<CompletableFuture<String>> recco2CallsFeatures = IntStream.range(1, RECCO2_CALLS_PER_CLIENT).boxed()
            .map(it -> {
                     log.info("Executing call no " + recco2CallsCounter.getAndIncrement());
                     return recco2DataRepository.callRecco2(1);
                 }
            ).collect(toList());

        CompletableFuture<List<String>> singleFeature = Traversables.traverse(recco2CallsFeatures);

        singleFeature.join();
        log.info("finished processing client: " + clientNo);
    }
}

@Slf4j
class Recco2DataRepository {
    ExecutorService executor = Executors.newFixedThreadPool(4);
    HttpClient client =
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .version(HttpClient.Version.HTTP_2)
            .proxy(ProxySelector.getDefault())
            .executor(executor).build();

    public CompletableFuture<String> callRecco2(Integer counter) {
        return client.sendAsync(varRequest(), HttpResponse.BodyHandlers.ofInputStream())
            .thenApply(response -> {
                log.info(String.format("Call to %s returned status %d", response.request().uri(), response.statusCode()));
                return "Response code: " + response.statusCode();
            })
            .exceptionally(ex -> {
                if (counter <= 1) {
                    log.warn("doing retry after error", ex);
                    return callRecco2(counter + 1).join();
                }
                log.error("Error ", ex);
                return "exeption";
            });
    }

    @SneakyThrows
    private HttpRequest varRequest() {
        String path = BackpressureTests.class.getResource("/body.json").getPath();
        String body = Files.readString(Paths.get(path));

        URI uri = new URI("https://api.dev.edge-lab.ch/recco/v2/risk-measures/VAR/granularities/contributions");
        return getBuilder(uri)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body.getBytes()))
            .build();
    }

    private HttpRequest.Builder getBuilder(URI uri) {
        return HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofSeconds(60))
            .setHeader("Accept", "application/json;charset=UTF-8")
            .setHeader("Content-Type", "application/json;charset=UTF-8")
            .setHeader("Accept-Encoding", "gzip")
            .setHeader("apikey", "XXX");
    }
}

/*a class in montblanc*/
class Traversables {

    public Traversables() {
        // no instance
    }

    public static <T> CompletableFuture<List<T>> traverse(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(unused -> futures.stream().map(CompletableFuture::join).collect(toList()));
    }

}
