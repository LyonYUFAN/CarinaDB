//package com.jiashi.db.engine.wal;
//
//import com.jiashi.db.common.model.LogRecord;
//import com.jiashi.db.common.coder.LogRecordCoder;
//import com.jiashi.db.common.model.LogRecordType;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * WAL 全链路极限测试：正常写入 -> 正常恢复 -> 物理破坏 -> 宕机恢复与截断
// */
//public class WalRecoveryTest {
//
//    private static final String TEST_DIR = "./carina-test-data";
//    private static final String TEST_FILE = "wal_test.log";
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("====== CarinaDB WAL 物理防御链路测试开始 ======");
//        prepareTestDir();
//
//        // ---------------------------------------------------------
//        // 阶段 1：模拟正常引擎运行，写入 10 条数据
//        // ---------------------------------------------------------
//        System.out.println("\n[阶段 1] 引擎启动，正常写入 10 条数据...");
//        WAL walWriter = new WAL(TEST_DIR, TEST_FILE);
//
//        for (int i = 1; i <= 10; i++) {
//            byte[] key = ("USER_" + i).getBytes();
//            byte[] value = ("VALUE_" + i).getBytes();
//            // 假设你有一个构造函数：LogRecord(LogRecordType type, byte[] key, byte[] value)
//            LogRecord record = new LogRecord(LogRecordType.PUT_KV, key, value);
//
//            // 使用你的 Coder 将 Record 序列化并封装成带 CRC Header 的 ByteBuffer
//            ByteBuffer encodedData = LogRecordCoder.encode(record);
//            walWriter.append(encodedData);
//        }
//
//        // 等待一小会儿让后台异步 Group Commit 落盘，然后安全关闭
//        Thread.sleep(500);
//        walWriter.close();
//        System.out.println("✅ 阶段 1 完成：已安全关闭 WAL。");
//
//        // ---------------------------------------------------------
//        // 阶段 2：模拟正常重启，验证数据完好
//        // ---------------------------------------------------------
//        System.out.println("\n[阶段 2] 引擎正常重启，执行恢复逻辑...");
//        WAL walReader1 = new WAL(TEST_DIR, TEST_FILE);
//        List<LogRecord> recoveredList1 = new ArrayList<>();
//
//        walReader1.recover(record -> recoveredList1.add(record));
//        walReader1.close();
//
//        if (recoveredList1.size() == 10) {
//            System.out.println("✅ 阶段 2 完成：成功恢复 10 条数据，无损坏！");
//        } else {
//            System.err.println("❌ 阶段 2 失败：数据数量不对，只恢复了 " + recoveredList1.size() + " 条！");
//        }
//
//        // ---------------------------------------------------------
//        // 阶段 3：人为制造物理灾难 (断电撕裂写 Torn Write)
//        // ---------------------------------------------------------
//        System.out.println("\n[阶段 3] 黑客行动：向文件尾部强行追加 12 字节的垃圾数据 (模拟写一半断电)...");
//        File logFile = new File(TEST_DIR, TEST_FILE);
//        long originalSize = logFile.length();
//
//        try (FileOutputStream fos = new FileOutputStream(logFile, true)) { // true 表示追加模式
//            fos.write(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C});
//        }
//        long corruptedSize = logFile.length();
//        System.out.println("⚠️ 物理破坏完成。文件原大小: " + originalSize + ", 被破坏后大小: " + corruptedSize);
//
//        // ---------------------------------------------------------
//        // 阶段 4：断电废墟中的终极恢复与截断 (Truncate)
//        // ---------------------------------------------------------
//        System.out.println("\n[阶段 4] 引擎灾难重启，检验 WAL 防御系统...");
//        WAL walReader2 = new WAL(TEST_DIR, TEST_FILE);
//        List<LogRecord> recoveredList2 = new ArrayList<>();
//
//        // 这里你应该能在控制台看到你写的 "WARN: Incomplete WAL header..." 或者 "Reached EOF..." 的红字警告！
//        walReader2.recover(record -> recoveredList2.add(record));
//        walReader2.close();
//
//        long finalSize = logFile.length();
//        System.out.println("\n====== 最终验尸报告 ======");
//        System.out.println("预期恢复条数: 10，实际恢复条数: " + recoveredList2.size());
//        System.out.println("文件是否被成功截断回原样？ " + (finalSize == originalSize ? "✅ 是的！" : "❌ 没有！当前大小: " + finalSize));
//
//        if (recoveredList2.size() == 10 && finalSize == originalSize) {
//            System.out.println("\n🎉🎉🎉 测试完美通过！你的 WAL 具备了真正的工业级防撕裂机制！");
//        }
//    }
//
//    private static void prepareTestDir() {
//        File dir = new File(TEST_DIR);
//        if (!dir.exists()) {
//            dir.mkdirs();
//        }
//        File file = new File(dir, TEST_FILE);
//        if (file.exists()) {
//            file.delete(); // 每次测试前清空历史包袱
//        }
//    }
//}