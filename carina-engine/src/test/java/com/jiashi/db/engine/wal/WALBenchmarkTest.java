package com.jiashi.db.engine.wal;

import com.jiashi.db.common.coder.LogRecordCoder;
import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.common.model.LogRecordType;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class WALBenchmarkTest {

    private static final String TEST_DIR = "target/wal_test";
    private static final String TEST_FILE = "benchmark.wal";

    // 模拟 100 个并发连接
    private static final int THREAD_COUNT = 100;
    // 每个连接写入 1000 条数据
    private static final int REQUESTS_PER_THREAD = 1000;
    private static final int TOTAL_REQUESTS = THREAD_COUNT * REQUESTS_PER_THREAD;

    public static void main(String[] args) throws Exception {
        // 1. 准备测试环境
        Path dirPath = Paths.get(TEST_DIR);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        File file = new File(TEST_DIR, TEST_FILE);
        if (file.exists()) {
            file.delete(); // 清理旧数据
        }

        System.out.println("Starting WAL Group Commit Benchmark...");
        System.out.println("Threads: " + THREAD_COUNT + ", Total Requests: " + TOTAL_REQUESTS);

        // 2. 初始化 WAL
        WAL wal = new WAL(TEST_DIR, TEST_FILE);

        // 3. 准备并发工具
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch readyLatch = new CountDownLatch(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger failureCount = new AtomicInteger(0);

        // 4. 提交并发任务
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                readyLatch.countDown(); // 报告当前线程已就绪
                try {
                    startLatch.await(); // 等待发令枪响，保证绝对并发

                    for (int j = 0; j < REQUESTS_PER_THREAD; j++) {
                        // 构造测试数据
                        byte[] key = ("user:" + threadId + ":" + j).getBytes();
                        byte[] value = ("data_payload_" + System.currentTimeMillis()).getBytes();

                        // 事实校准：使用你定义的 PUT_KV 类型，明确表示这是一个纯 KV 写入，不需要 vector
                        LogRecord record = new LogRecord(LogRecordType.PUT_KV, key, value, null);

                        // 序列化
                        ByteBuffer buffer = LogRecordCoder.encode(record);
                        // 事实：allocate 分配的内存大小刚好等于数据大小，可以直接 array() 零拷贝获取
                        ByteBuffer dataBytes = LogRecordCoder.encode(record);
                        // 核心操作：追加到 WAL
                        wal.append(dataBytes);
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown(); // 报告当前线程执行完毕
                }
            });
        }

        // 5. 等待所有线程就绪后，打响发令枪并开始计时
        readyLatch.await();
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // 6. 等待所有写操作完成并停止计时
        doneLatch.await();
        long endTime = System.currentTimeMillis();

        wal.close();
        executor.shutdown();

        // 7. 统计与输出客观事实数据
        long totalTimeMs = endTime - startTime;
        double tps = (TOTAL_REQUESTS / (double) totalTimeMs) * 1000;
        long fileSize = file.length();

        System.out.println("=========================================");
        System.out.println("Benchmark Finished!");
        System.out.println("Total Time Spent: " + totalTimeMs + " ms");
        System.out.println("Throughput (TPS): " + String.format("%.2f", tps) + " ops/sec");
        System.out.println("Failure Count: " + failureCount.get());
        System.out.println("Total File Size: " + (fileSize / 1024.0 / 1024.0) + " MB");
        System.out.println("=========================================");

        // 8. 灾难恢复 (Crash Recovery) 准确性与性能验证
        // =================================================================
        System.out.println("\nStarting WAL Recovery Verification...");
        WAL recoveryWal = new WAL(TEST_DIR, TEST_FILE);
        AtomicInteger recoveredCount = new AtomicInteger(0);
        long recoverStartTime = System.currentTimeMillis();

        // 执行恢复，将解析出的每一条 record 传入 lambda 表达式进行计数
        recoveryWal.recover(record -> {
            recoveredCount.incrementAndGet();
            // 在这里，如果是真实的数据库启动，你会将 record 重新写入 MemTable
        });

        long recoverEndTime = System.currentTimeMillis();
        System.out.println("=========================================");
        System.out.println("Recovery Finished!");
        System.out.println("Recovery Time Spent: " + (recoverEndTime - recoverStartTime) + " ms");
        System.out.println("Expected Records: " + TOTAL_REQUESTS);
        System.out.println("Recovered Records: " + recoveredCount.get());

        // 事实断言：写入量与恢复量必须绝对一致
        boolean isConsistent = (TOTAL_REQUESTS == recoveredCount.get());
        System.out.println("Data Consistency Check: " + (isConsistent ? "PASS ✔️" : "FAIL ❌"));
        System.out.println("=========================================");

        recoveryWal.close();
    }
}