package com.jiashi.db.engine;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.engine.memtable.MemTable;
import com.jiashi.db.engine.sstable.SSTableBuilder;
import com.jiashi.db.engine.sstable.SSTableReader;
import com.jiashi.db.engine.wal.WAL;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 存储引擎全局大脑：负责 MemTable 调度、后台刷盘与读写管线
 */
public class CarinaEngine {

    private final String dbDirectory;
    private final AtomicInteger sstFileIdGenerator;

    // ======== 内存流转区 ========
    private volatile MemTable activeMemTable;
    private final List<MemTable> immutableMemTables = new CopyOnWriteArrayList<>();

    // ======== 全局极值地图 ========
    private final List<SSTableReader> ssTables = new CopyOnWriteArrayList<>();

    // ======== 后台调度器 ========
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();

    // 宏观配置
    private static final int MEMTABLE_CAPACITY = 64 * 1024 * 1024; // 64MB

    public CarinaEngine(String dbDirectory) throws IOException {
        this.dbDirectory = dbDirectory;
        Path dirPath = Paths.get(dbDirectory);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        // 1. 启动事实：极速扫盘，建立极值地图
        loadSSTables();

        // 2. 恢复文件版本号
        int maxId = extractMaxFileId();
        this.sstFileIdGenerator = new AtomicInteger(maxId);

        // 3. 初始化第一个活跃内存表
        this.activeMemTable = createNewMemTable();

        recoverFromOldWals();

        System.out.println("CarinaDB 启动成功，已挂载 " + ssTables.size() + " 个 SSTable。");
    }

    /**
     * 扫盘与加载极值地图
     * @throws IOException
     */
    private void loadSSTables() throws IOException {
        try (Stream<Path> paths = Files.list(Paths.get(dbDirectory))) {
            List<Path> sstPaths = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".sst"))
                    // 物理事实法则：文件号越大代表数据越新，降序排列
                    .sorted(Comparator.comparing(Path::getFileName, Comparator.reverseOrder()))
                    .collect(Collectors.toList());

            for (Path sstPath : sstPaths) {
                SSTableReader reader = new SSTableReader(sstPath);
                ssTables.add(reader);
            }
        }
    }

    /**
     * 分配具有独立 WAL 的新 MemTable
     */
    private MemTable createNewMemTable() throws IOException {
        // 利用时间戳或唯一序列号保证 WAL 文件名不冲突
        String walFileName = "wal_" + System.currentTimeMillis() + ".log";
        return new MemTable(MEMTABLE_CAPACITY, dbDirectory, walFileName);
    }

    /**
     * 核心写链路 (Put)
     */
    public void put(byte[] key, byte[] value) throws IOException {
        // 1. 尝试写入当前 Active MemTable
        boolean success = activeMemTable.put(key, value);

        // 2. 如果返回 false，说明物理水位已达 90% 且被内部 freeze，必须切换
        if (!success) {
            switchMemTable();
            // 切换完成后，重试写入全新的 MemTable (不考虑单挑 KV 超过 64MB 的极端情况)
            activeMemTable.put(key, value);
        }
    }

    /**
     * 核心状态翻转与触发刷盘
     */
    private synchronized void switchMemTable() throws IOException {
        // Double-check 防止并发写线程引发的重复切换
        if (!activeMemTable.isImmutable()) {
            activeMemTable.freeze(); // 强制冻结，补齐状态机
        }

        // 1. 退居二线
        MemTable tableToFlush = this.activeMemTable;
        immutableMemTables.add(tableToFlush);

        // 2. 瞬间挂载新的接客 MemTable
        this.activeMemTable = createNewMemTable();

        // 3. 异步提交物理落盘任务
        flushExecutor.submit(() -> {
            try {
                flushMemTableToDisk(tableToFlush);
            } catch (IOException e) {
                System.err.println("后台刷盘致命失败: " + e.getMessage());
            }
        });
    }

    /**
     * 真实的后台流式落盘逻辑
     */
    private void flushMemTableToDisk(MemTable memTable) throws IOException {
        int newFileId = sstFileIdGenerator.incrementAndGet();
        String fileName = String.format("%06d.sst", newFileId);
        Path sstPath = Paths.get(dbDirectory, fileName);

        // 1. 召唤包工头。由于 MemTable 没暴露 size()，这里根据 64MB 经验预估约 10 万条 KV
        int estimatedInsertions = 100000;
        SSTableBuilder builder = new SSTableBuilder(sstPath, estimatedInsertions);

        // 2. 流式铺铁轨：遍历 Immutable MemTable 暴露的 LogRecord
        Iterator<LogRecord> iterator = memTable.iterator();
        while (iterator.hasNext()) {
            LogRecord record = iterator.next();
            // 将 Record 解析为底层的 key/value 喂给 Builder
            // 如果是 DELETE 墓碑，value 本身就是 byte[0]，Builder 也会忠实记录
            builder.add(record.getKey(), record.getValue());
        }

        // 3. 终极结账：追加元数据和 40 字节 Footer
        builder.finish();

        // 4. 将新生成的死文件唤醒为极值地图节点
        SSTableReader newReader = new SSTableReader(sstPath);

        // 极其重要的物理事实：新生成的文件包含最新的数据，必须插在 List 的最前面！
        ssTables.add(0, newReader);

        // 5. 功成身退：清理内存和废弃的 WAL
        immutableMemTables.remove(memTable);
        memTable.close(); // 释放堆外内存和关闭旧日志

        System.out.println("Flush 完成：生成新段文件 " + fileName);
    }

    /**
     * 核心读链路 (Get) - 严格按照新旧时序的瀑布流
     */
    public byte[] get(byte[] key) throws IOException {
        // 1. 查最热的内存 (Active)
        byte[] value = activeMemTable.get(key);
        if (value != null) return value;

        // 2. 查温热的内存 (Immutable，倒序遍历保证查到最新的修改)
        for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
            value = immutableMemTables.get(i).get(key);
            if (value != null) return value;
        }

        // 3. 查冷热交替的磁盘极值地图 (SSTables)
        for (SSTableReader reader : ssTables) {
            // 利用极小常驻内存做 O(1) 拦截
            if (compareBytes(key, reader.getMinKey()) < 0 || compareBytes(key, reader.getMaxKey()) > 0) {
                continue; // 范围不命中，绝对不碰磁盘
            }

            // 范围命中，触发底层三级按需缓存防线
            value = reader.searchBinary(key);
            if (value != null) return value;
        }

        return null;
    }

    private int extractMaxFileId() {
        if (ssTables.isEmpty()) return 0;
        String fileName = ssTables.get(0).getFileId();
        try {
            return Integer.parseInt(fileName.replace(".sst", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private void recoverFromOldWals() throws IOException {
        System.out.println("🚀 开始扫描并回放历史 WAL 日志...");
        try(Stream<Path> paths = Files.list(Paths.get(dbDirectory))){
            List<Path> walPaths = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().contains("wal_") && p.toString().endsWith(".log"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .collect(Collectors.toList());

            if (walPaths.isEmpty()) {
                System.out.println("✅ 没有发现遗留的 WAL 日志，无需恢复。");
                return;
            }

            for (Path walPath : walPaths) {
                String oldFileName = walPath.getFileName().toString();
                if (oldFileName.equals(activeMemTable.getWalFileName())) {
                    continue;
                }
                WAL wal = new WAL(dbDirectory,oldFileName);
                wal.recover(record -> {
                            activeMemTable.restoreFromWal(record);
                    }
                );
            }
        }
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len1 = a.length;
        int len2 = b.length;
        int lim = Math.min(len1, len2);
        for (int i = 0; i < lim; i++) {
            int c1 = a[i] & 0xFF;
            int c2 = b[i] & 0xFF;
            if (c1 != c2) { return c1 - c2; }
        }
        return len1 - len2;
    }

    public void close() {
        System.out.println("开始执行 CarinaDB 安全停机...");
        try {
            // 1. 强制将最后一个活跃内存表推入刷盘队列
            if (activeMemTable != null && !activeMemTable.isImmutable()) {
                activeMemTable.freeze();
                immutableMemTables.add(activeMemTable);
                flushExecutor.submit(() -> {
                    try {
                        flushMemTableToDisk(activeMemTable);
                    } catch (IOException e) {
                        System.err.println("停机前刷盘失败: " + e.getMessage());
                    }
                });
            }

            // 2. 优雅关闭线程池并阻塞等待 (工业级事实标准做法)
            flushExecutor.shutdown();
            if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("警告：部分刷盘任务超时未能完成！");
            }

            // 3. 安全关闭所有物理文件句柄
            for (SSTableReader reader : ssTables) {
                reader.close();
            }
            System.out.println("CarinaDB 安全停机完毕。");

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}