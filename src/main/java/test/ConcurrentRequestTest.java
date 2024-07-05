package test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentRequestTest {

    private static ExecutorService executorService;
    private static final int THREAD_COUNT = 10000; // 并发线程数
    private static final String TEST_URL = "http://localhost:9008/EasydbServer/easydb"; // 你的Servlet地址
    private static final AtomicInteger requestCounter = new AtomicInteger(0);
    @BeforeAll
    public static void setUp() {
        // 初始化线程池
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterAll
    public static void tearDown() throws InterruptedException {
        // 关闭线程池
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
    }

    @Test
    public static void testConcurrentHttpRequests() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    sendRequest(TEST_URL);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有任务完成
        latch.await();

        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken for " + THREAD_COUNT + " requests: " + (endTime - startTime) + "ms");
    }


    private static void sendRequest(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");

        // 使用线程安全的方式生成唯一的请求计数，并构造带有该计数的请求体
        int requestNumber = requestCounter.incrementAndGet();
        String requestBody = "{\"key\":\"testKey" + requestNumber + "\",\"value\":\"testValue" + requestNumber + "\"}";
        byte[] outputInBytes = requestBody.getBytes("UTF-8");
        OutputStream os = connection.getOutputStream();
        os.write(outputInBytes);
        os.close();

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("Failed : HTTP error code : " + responseCode);
        }

        // 读取响应等操作省略
        connection.disconnect();
    }
public static void main(String[] args) throws InterruptedException {
    setUp(); // 初始化线程池
    try {
        testConcurrentHttpRequests(); // 执行并发HTTP请求测试
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // 恢复中断状态
        throw e;
    } finally {
        tearDown(); // 确保线程池关闭
    }
}

}
