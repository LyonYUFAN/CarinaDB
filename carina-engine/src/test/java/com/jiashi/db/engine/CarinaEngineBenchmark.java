package com.jiashi.db.engine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 物理级暴力压测器：见证 CarinaDB 绞肉机的轰鸣
 */
public class CarinaEngineBenchmark {

    // ================= 压测物理配置 =================
    private static final String DB_DIR = "carina-benchmark-data";
    // 💥 暴力参数：20万条数据，足以打满好几个 64MB 的 MemTable，强制触发各种后台行为
    private static final int TOTAL_RECORDS = 200_000;
    // 💥 并发参数：32 个独立线程同时对内存跳表发起冲锋
    private static final int THREAD_COUNT = 32;
    // 💥 向量维度：128 维 (每个向量占 512 字节)，模拟真实的大模型 Embedding
    private static final int VECTOR_DIM = 128;

    public static void main(String[] args) throws Exception {
        System.out.println("==================================================");
        System.out.println("🚀 启动 CarinaDB WiscKey 暴力压测程序 🚀");
        System.out.println("==================================================");
        System.out.println("配置参数:");
        System.out.println(" - 压测数据量: " + TOTAL_RECORDS + " 条");
        System.out.println(" - 并发线程数: " + THREAD_COUNT);
        System.out.println(" - 向量维度:   " + VECTOR_DIM + "D");
        System.out.println("--------------------------------------------------\n");

        // 1. 物理清场，确保是从零开始压测
        cleanUpOldData(DB_DIR);

        CarinaEngine engine = new CarinaEngine(DB_DIR);
        Thread.sleep(1000); // 让后台线程稍微热个身

        try {
            // ==========================================
            // ⚔️ 战役一：极其暴力的并发写入 (Put)
            // ==========================================
            System.out.println("\n[战役一] 开始高并发混合写入...");
            ExecutorService writePool = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch writeStartGate = new CountDownLatch(1); // 发令枪
            CountDownLatch writeEndGate = new CountDownLatch(THREAD_COUNT); // 终点线

            int recordsPerThread = TOTAL_RECORDS / THREAD_COUNT;
            AtomicInteger failedWrites = new AtomicInteger(0);

            for (int i = 0; i < THREAD_COUNT; i++) {
                final int threadId = i;
                writePool.submit(() -> {
                    try {
                        writeStartGate.await(); // 极其残忍：所有线程在这里屏住呼吸，等待统一发令
                        for (int j = 0; j < recordsPerThread; j++) {
                            int globalId = threadId * recordsPerThread + j;
                            byte[] key = ("key_" + globalId).getBytes();
                            byte[] value = ("value_data_payload_for_" + globalId).getBytes();

                            // 随机决定是写向量还是纯 KV (80% 概率写向量)
                            float[] vector = null;
                            if (Math.random() < 0.8) {
                                vector = generateRandomVector(VECTOR_DIM);
                            }

                            engine.put(key, value, vector);
                        }
                    } catch (Exception e) {
                        failedWrites.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        writeEndGate.countDown();
                    }
                });
            }

            long writeStartTime = System.currentTimeMillis();
            writeStartGate.countDown(); // 💥 砰！发令枪响，万马奔腾！
            writeEndGate.await();       // 等待所有写入完成
            long writeEndTime = System.currentTimeMillis();
            writePool.shutdown();

            long writeCostTime = writeEndTime - writeStartTime;
            double writeQps = (TOTAL_RECORDS * 1000.0) / writeCostTime;

            System.out.println("\n📊 [写入战报]");
            System.out.println("✅ 总耗时: " + writeCostTime + " ms");
            System.out.println("🚀 写入吞吐量 (QPS): " + String.format("%.2f", writeQps) + " ops/sec");
            System.out.println("❌ 失败次数: " + failedWrites.get());

            // 让系统喘口气，观察控制台里的 L0 刷盘和 Compaction 日志
            System.out.println("\n⏳ 等待 5 秒，让后台绞肉机(Compaction)飞一会儿...");
            Thread.sleep(5000);

            // ==========================================
            // ⚔️ 战役二：WiscKey 二次寻址读写 (Query)
            // ==========================================
            System.out.println("\n[战役二] 开始随机点查与向量打捞测试...");
            int readCount = 50_000;
            Random random = new Random();
            int hitCount = 0;
            int vectorHitCount = 0;

            long readStartTime = System.currentTimeMillis();
            for (int i = 0; i < readCount; i++) {
                // 随机抽取之前写过的 Key
                int randomId = random.nextInt(TOTAL_RECORDS);
                byte[] queryKey = ("key_" + randomId).getBytes();

                CarinaEngine.QueryResult result = engine.query(queryKey);

                if (result != null) {
                    hitCount++;
                    if (result.vector != null) {
                        vectorHitCount++;
                    }
                }
            }
            long readEndTime = System.currentTimeMillis();
            long readCostTime = readEndTime - readStartTime;
            double readQps = (readCount * 1000.0) / readCostTime;

            System.out.println("\n📊 [读取战报]");
            System.out.println("✅ 总耗时: " + readCostTime + " ms (包含去 vLog 打捞向量的物理 I/O)");
            System.out.println("🚀 读取吞吐量 (QPS): " + String.format("%.2f", readQps) + " ops/sec");
            System.out.println("🎯 命中率: " + hitCount + " / " + readCount);
            System.out.println("🎯 成功捞出大向量次数: " + vectorHitCount);

        } finally {
            // 停机并统计物理磁盘占用
            engine.close();
            printDirectorySize(DB_DIR);
        }
    }

    // ================== 辅助工具方法 ==================

    private static float[] generateRandomVector(int dim) {
        float[] vec = new float[dim];
        Random rand = new Random();
        for (int i = 0; i < dim; i++) {
            vec[i] = rand.nextFloat();
        }
        return vec;
    }

    private static void cleanUpOldData(String dirPath) {
        File dir = new File(dirPath);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
        }
    }

    private static void printDirectorySize(String dirPath) {
        try {
            Path folder = Paths.get(dirPath);
            long sizeBytes = Files.walk(folder)
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
            double sizeMB = sizeBytes / (1024.0 * 1024.0);
            System.out.println("\n💾 [物理引擎收工状态]");
            System.out.println("📂 数据库目录: " + dirPath);
            System.out.println("📦 最终磁盘占用: " + String.format("%.2f", sizeMB) + " MB");

            // 打印下各类型文件的数量
            File[] sstFiles = new File(dirPath).listFiles((d, n) -> n.endsWith(".sst"));
            File[] vlogFiles = new File(dirPath).listFiles((d, n) -> n.startsWith("vlog-"));
            System.out.println("📄 现存 SSTable 文件数: " + (sstFiles != null ? sstFiles.length : 0));
            System.out.println("🎞️ 现存 vLog 向量仓库数: " + (vlogFiles != null ? vlogFiles.length : 0));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}