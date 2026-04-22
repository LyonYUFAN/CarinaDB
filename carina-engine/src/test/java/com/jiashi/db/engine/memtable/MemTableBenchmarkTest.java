//package com.jiashi.db.engine.memtable;
//
//import java.io.File;
//import java.nio.charset.StandardCharsets;
//import java.util.Arrays;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicInteger;
//
///**
// * 工业级 LSM-Tree MemTable 端到端物理压测 (包含向量混合负载)
// */
//public class MemTableBenchmarkTest {
//
//    // 压测参数配置事实
//    private static final int THREAD_COUNT = 50;       // 模拟 50 个并发客户端
//    private static final int TOTAL_REQUESTS = 500000; // 总计写入 50 万条数据
//
//    public static void main(String[] args) throws Exception {
//        // 1. 物理环境清理与准备
//        String walDir = "./test_data";
//        File dir = new File(walDir);
//        if (!dir.exists()) dir.mkdirs();
//        File walFile = new File(dir, "benchmark.wal");
//        if (walFile.exists()) walFile.delete(); // 每次测试前清空历史 WAL
//
//        // 事实：分配 256MB 的物理堆外内存
//        int capacity = 256 * 1024 * 1024;
//        MemTable memTable = new MemTable(capacity, walDir, "benchmark.wal");
//
//        System.out.println("=========================================");
//        System.out.println("1. 开始基础物理链路正确性校验 (KV + Vector)...");
//        testCorrectness(memTable);
//
//        System.out.println("\n=========================================");
//        System.out.println("2. 开始极致并发吞吐量压测 (混合 10% 向量写入负载)...");
//        testPerformance(memTable);
//
//        // 3. 物理资源安全释放
//        memTable.close();
//        System.out.println("\n压测结束，WAL 日志文件已生成，大小: " + walFile.length() / 1024 / 1024 + " MB");
//    }
//
//    /**
//     * 第一阶段：验证读、写、删、以及向量存储的内存对齐正确性
//     */
//    private static void testCorrectness(MemTable memTable) {
//        // --- 测试 1: 纯 KV 写入与读取 ---
//        byte[] key1 = "user:001".getBytes(StandardCharsets.UTF_8);
//        byte[] val1 = "Alice".getBytes(StandardCharsets.UTF_8);
//        memTable.put(key1, val1);
//        System.out.println(" -> 写入普通 KV [user:001 = Alice] 成功");
//
//        if (Arrays.equals(val1, memTable.get(key1))) {
//            System.out.println(" -> 读取普通 KV 正确");
//        } else {
//            System.err.println(" -> 致命错误：读取普通 KV 失败！");
//        }
//
//        // --- 测试 2: 带有向量的高维数据写入与读取验证 ---
//        byte[] vecKey = "image:001".getBytes(StandardCharsets.UTF_8);
//        byte[] vecVal = "metadata_cat_image".getBytes(StandardCharsets.UTF_8);
//        // 模拟一个 5 维的图片特征向量
//        float[] vector = {0.12f, -0.45f, 0.88f, 1.0f, -0.01f};
//
//        // 核心测试：触发带有 float[] 的物理内存切分和写入
//        memTable.putVector(vecKey, vecVal, vector);
//        System.out.println(" -> 写入向量特征 [image:001] (维度: " + vector.length + ") 成功");
//
//        // 物理事实验证：如果向量数据的写入导致了内存越界，此时去读 value 必然会读出乱码或宕机
//        byte[] readVecVal = memTable.get(vecKey);
//        if (Arrays.equals(vecVal, readVecVal)) {
//            System.out.println(" -> 读取向量记录的 Value 成功！证明堆外内存布局对齐完全正确，未被破坏。");
//        } else {
//            System.err.println(" -> 致命错误：读取向量记录的 Value 失败！底层偏移量计算可能存在偏差。");
//        }
//
//        // --- 测试 3: 逻辑墓碑覆盖测试 ---
//        memTable.delete(vecKey);
//        System.out.println(" -> 发起逻辑删除 image:001");
//        if (memTable.get(vecKey) == null) {
//            System.out.println(" -> 验证墓碑生效，image:001 已彻底被掩蔽");
//        }
//    }
//
//    /**
//     * 第二阶段：多线程并发暴力写入压测
//     */
//    private static void testPerformance(MemTable memTable) throws InterruptedException {
//        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
//        CountDownLatch startLatch = new CountDownLatch(1);
//        CountDownLatch endLatch = new CountDownLatch(TOTAL_REQUESTS);
//        AtomicInteger failureCount = new AtomicInteger(0);
//
//        System.out.println(" -> 正在预热线程池，准备发令枪...");
//
//        for (int i = 0; i < TOTAL_REQUESTS; i++) {
//            final int index = i;
//            executor.submit(() -> {
//                try {
//                    startLatch.await(); // 等待起跑信号
//
//                    byte[] key = ("bench_key_" + index).getBytes(StandardCharsets.UTF_8);
//                    byte[] val = ("payload_data_" + index).getBytes(StandardCharsets.UTF_8);
//                    boolean success;
//
//                    // 事实：模拟真实业务场景，每 10 条数据中就有 1 条是包含 128 维特征向量的重型数据
//                    if (index % 10 == 0) {
//                        float[] dummyVector = new float[128];
//                        dummyVector[0] = index * 0.1f; // 随便填入一个浮点数模拟负载
//                        success = memTable.putVector(key, val, dummyVector);
//                    } else {
//                        success = memTable.put(key, val);
//                    }
//
//                    if (!success) {
//                        failureCount.incrementAndGet();
//                    }
//                } catch (Exception e) {
//                    failureCount.incrementAndGet();
//                } finally {
//                    endLatch.countDown();
//                }
//            });
//        }
//
//        long startTime = System.currentTimeMillis();
//        startLatch.countDown(); // 发令枪响
//        endLatch.await();       // 等待所有线程跑完
//        long endTime = System.currentTimeMillis();
//
//        executor.shutdown();
//
//        long costTimeMs = endTime - startTime;
//        double tps = (TOTAL_REQUESTS * 1000.0) / costTimeMs;
//
//        System.out.println(" -> 压测配置: " + THREAD_COUNT + " 线程并发");
//        System.out.println(" -> 总写入量: " + TOTAL_REQUESTS + " 条 (包含 10% 的 128维向量记录)");
//        System.out.println(" -> 物理总耗时: " + costTimeMs + " 毫秒");
//        System.out.printf(" -> 极限吞吐量 (TPS): %.2f 条/秒\n", tps);
//
//        if (failureCount.get() > 0) {
//            System.err.println(" -> 警告: 压测中出现了 " + failureCount.get() + " 次写入失败 (可能触及了内存阈值)！");
//        }
//    }
//}