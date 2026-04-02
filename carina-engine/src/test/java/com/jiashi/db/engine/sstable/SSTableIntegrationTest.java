package com.jiashi.db.engine.sstable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SSTableIntegrationTest {

    private Path tempSstFile;
    // 性能测试：数据量拉升至 100 万条 (预计生成近百兆文件)
    private static final int BENCHMARK_RECORD_COUNT = 1000000;
    // 性能测试：随机抽查 10000 次
    private static final int SEARCH_COUNT = 10000;

    @BeforeEach
    void setUp() throws IOException {
        tempSstFile = Files.createTempFile("carinadb-test-", ".sst");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tempSstFile);
    }

    /**
     * 基础正确性测试 (保留原有逻辑，确保底线不破)
     */
    @Test
    void testSSTableWriteAndReadCorrectness() throws IOException {
        int recordCount = 10000;
        SSTableBuilder builder = new SSTableBuilder(tempSstFile);

        for (int i = 0; i < recordCount; i++) {
            String keyStr = String.format("key_%07d", i);
            byte[] value = buildMockValue(i);
            builder.add(keyStr.getBytes(), value);
        }
        builder.finish();

        assertTrue(Files.size(tempSstFile) > 0);

        // 验证魔数
        try (FileChannel probeChannel = FileChannel.open(tempSstFile, StandardOpenOption.READ)) {
            ByteBuffer magicBuffer = ByteBuffer.allocate(8);
            probeChannel.position(probeChannel.size() - 8);
            probeChannel.read(magicBuffer);
            magicBuffer.flip();
            assertEquals(Footer.MAGIC_NUMBER, magicBuffer.getLong());
        }

        SSTableReader reader = new SSTableReader(tempSstFile);
        assertNotNull(reader.searchBinary("key_0008500".getBytes()));
        assertNull(reader.searchBinary("key_9999999".getBytes()));
        reader.close();
    }

    /**
     * 核心性能对决：线性扫描 VS 二分查找
     */
    @Test
    void testSearchPerformanceBenchmark() throws IOException {
        System.out.println("========== SSTable 性能基准测试开始 ==========");

        // 1. 构造海量数据文件 (这一步可能需要几秒到十几秒，取决于 SSD 性能)
        System.out.println("[1] 正在生成 " + BENCHMARK_RECORD_COUNT + " 条数据的 SSTable 文件...");
        long buildStart = System.currentTimeMillis();
        SSTableBuilder builder = new SSTableBuilder(tempSstFile);
        for (int i = 0; i < BENCHMARK_RECORD_COUNT; i++) {
            builder.add(String.format("key_%07d", i).getBytes(), buildMockValue(i));
        }
        builder.finish();
        long buildTime = System.currentTimeMillis() - buildStart;
        System.out.println("    文件生成完毕，耗时: " + buildTime + " ms，文件大小: " + (Files.size(tempSstFile) / 1024 / 1024) + " MB");

        // 2. 准备完全随机的测试查询词 (避免顺序查询命中操作系统预读缓存)
        List<byte[]> targetKeys = new ArrayList<>();
        Random random = new Random(42); // 固定种子保证每次测试数据一致
        for (int i = 0; i < SEARCH_COUNT; i++) {
            int targetId = random.nextInt(BENCHMARK_RECORD_COUNT);
            targetKeys.add(String.format("key_%07d", targetId).getBytes());
        }

        SSTableReader reader = new SSTableReader(tempSstFile);

        // 3. JVM 预热 (Warmup)：触发 JIT C2 编译器，剔除初始执行噪音
        System.out.println("[2] 正在进行 JVM JIT 预热...");
        for (int i = 0; i < 2000; i++) {
            reader.searchLinear(targetKeys.get(i));
            reader.searchBinary(targetKeys.get(i));
        }

        // 4. 正式开跑：线性查找 (O(n))
        System.out.println("[3] 开始测试 Linear Search (" + SEARCH_COUNT + " 次随机点查)...");
        long linearStart = System.nanoTime();
        for (byte[] key : targetKeys) {
            byte[] res = reader.searchLinear(key);
            assertNotNull(res); // 校验一下防止内部报错被掩盖
        }
        long linearTimeNanos = System.nanoTime() - linearStart;

        // 5. 正式开跑：二分查找 (O(log n))
        System.out.println("[4] 开始测试 Binary Search (" + SEARCH_COUNT + " 次随机点查)...");
        long binaryStart = System.nanoTime();
        for (byte[] key : targetKeys) {
            byte[] res = reader.searchBinary(key);
            assertNotNull(res);
        }
        long binaryTimeNanos = System.nanoTime() - binaryStart;

        // 6. 输出真实性能对比数据
        double linearMs = linearTimeNanos / 1_000_000.0;
        double binaryMs = binaryTimeNanos / 1_000_000.0;

        System.out.println("================= 测试结论 =================");
        System.out.printf("线性扫描 (O(n))    总耗时: %.2f ms%n", linearMs);
        System.out.printf("二分查找 (O(log n)) 总耗时: %.2f ms%n", binaryMs);

        if (binaryMs < linearMs) {
            System.out.printf("🚀 性能提升: %.2f 倍%n", linearMs / binaryMs);
        } else {
            System.out.println("⚠️ 数据量或系统环境异构，导致二分查找未体现出优势");
        }
        System.out.println("============================================");

        reader.close();
    }

    /**
     * 辅助方法：生成假数据
     */
    private byte[] buildMockValue(int i) {
        byte[] scalarData = "scalar_val".getBytes();
        ByteBuffer valBuffer = ByteBuffer.allocate(scalarData.length + 8);
        valBuffer.put(scalarData);
        valBuffer.putLong(100000L + i);
        return valBuffer.array();
    }
}