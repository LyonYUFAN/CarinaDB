package com.jiashi.db.engine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

/**
 * CarinaEngine 全链路极限基准测试
 */
public class CarinaEngineBenchmark {

    // 压测参数
    private static final int TOTAL_RECORDS = 200_000; // 20万条数据
    private static final int VALUE_SIZE = 1024;       // 1KB 的 Value，总计约 200MB 数据

    public static void main(String[] args) throws Exception {
        System.out.println("==================================================");
        System.out.println("🚀 CarinaEngine 极限压测启动");
        System.out.println("==================================================");

        // 1. 准备干净的测试目录
        String dbPath = "./carina-benchmark-data";
        cleanDirectory(Paths.get(dbPath));

        // 2. 启动引擎
        CarinaEngine engine = new CarinaEngine(dbPath);

        // ---------------------------------------------------------
        // 阶段 1：疯狂写入测试 (验证 64MB 阈值与流式落盘)
        // ---------------------------------------------------------
        System.out.println("\n[Phase 1] 开始压入 " + TOTAL_RECORDS + " 条 1KB 数据...");
        byte[] fixedValue = new byte[VALUE_SIZE];
        Arrays.fill(fixedValue, (byte) 'A'); // 用 'A' 填充 1KB

        long writeStartTime = System.currentTimeMillis();
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            byte[] key = String.format("USER_%08d", i).getBytes();
            engine.put(key, fixedValue);

            if (i > 0 && i % 50000 == 0) {
                System.out.println("  -> 已压入 " + i + " 条数据...");
            }
        }
        long writeEndTime = System.currentTimeMillis();
        long writeCost = writeEndTime - writeStartTime;
        System.out.println("✅ 写入完毕! 耗时: " + writeCost + " ms, TPS: " + (TOTAL_RECORDS * 1000L / writeCost) + " ops/s");

        // 稍微停顿，等待最后一个后台 flush 线程执行完毕 (工程事实：工业界有专门的 awaitFlush 机制，这里简易用 sleep)
        Thread.sleep(2000);

        // ---------------------------------------------------------
        // 阶段 2：全量精准点查 (验证 内存/磁盘 穿透与正确性)
        // ---------------------------------------------------------
        System.out.println("\n[Phase 2] 开始全量校验 " + TOTAL_RECORDS + " 条已存在数据...");
        long readStartTime = System.currentTimeMillis();
        int successCount = 0;

        for (int i = 0; i < TOTAL_RECORDS; i++) {
            byte[] key = String.format("USER_%08d", i).getBytes();
            byte[] value = engine.get(key);

            if (value != null && Arrays.equals(value, fixedValue)) {
                successCount++;
            } else {
                // System.err.println("❌ 数据校验失败! Key: USER_" + String.format("%08d", i));
            }
        }
        long readEndTime = System.currentTimeMillis();
        long readCost = readEndTime - readStartTime;
        System.out.println("✅ 校验命中率: " + successCount + " / " + TOTAL_RECORDS);
        System.out.println("✅ 读取耗时: " + readCost + " ms, QPS: " + (TOTAL_RECORDS * 1000L / readCost) + " ops/s");

        // ---------------------------------------------------------
        // 阶段 3：布隆过滤器抗压拦截测试 (查询 10万条不存在的数据)
        // ---------------------------------------------------------
        int missRecords = 100_000;
        System.out.println("\n[Phase 3] 开始进行 " + missRecords + " 条无效查询拦截测试...");
        long missStartTime = System.currentTimeMillis();
        int interceptedCount = 0;

        for (int i = 0; i < missRecords; i++) {
            // 生成绝对不存在的 Key
            byte[] key = String.format("GHOST_%08d", i).getBytes();
            byte[] value = engine.get(key);
            if (value == null) {
                interceptedCount++;
            }
        }
        long missEndTime = System.currentTimeMillis();
        long missCost = missEndTime - missStartTime;
        System.out.println("✅ 成功拦截: " + interceptedCount + " / " + missRecords);
        // 你会发现拦截的 QPS 极高，因为布隆过滤器全在内存里做了 O(1) 阻断
        System.out.println("✅ 拦截耗时: " + missCost + " ms, QPS: " + (missRecords * 1000L / missCost) + " ops/s");

        // 4. 平滑关闭
        System.out.println("\n==================================================");
        System.out.println("🎉 压测圆满结束，正在安全关闭引擎...");
        engine.close();

        // 打印磁盘占用
        long totalDiskSize = calculateDirectorySize(new File(dbPath));
        System.out.println("💾 磁盘物理占用总计: " + (totalDiskSize / 1024 / 1024) + " MB");
        System.out.println("==================================================");
    }

    // 清理老旧测试数据
    private static void cleanDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    // 计算文件夹大小
    private static long calculateDirectorySize(File dir) {
        long length = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) length += file.length();
                else length += calculateDirectorySize(file);
            }
        }
        return length;
    }
}