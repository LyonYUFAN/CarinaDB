package com.jiashi.db.engine.memtable;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.common.model.LogRecordType;
import com.jiashi.db.common.coder.LogRecordCoder; // 假设你有这个编码器
import com.jiashi.db.engine.blob.BlobWriter;
import com.jiashi.db.engine.wal.WAL;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LSM-Tree 核心内存组件：统一接管 WAL 日志与无锁跳表的写入时序
 */
public class MemTable implements Iterable<LogRecord> {
    // TODO(MVP-Debt): 状态切换权越界。
    // 当前由 MemTable 内部利用 CAS 自行维护 isImmutable 状态，这在引擎工程中属于职责倒置。
    // 未来演进：移除此字段及相关的 CAS 逻辑。
    // 内存写满的判断逻辑应上浮，由 CarinaEngine 的写入主链路 (Write Thread/Group Commit) 统一计算剩余空间。
    // 并在写满时由 Engine 执行 Write Stall (写停顿) 和 Pointer Switch (指针切换)。

    private final Arena arena;
    private final OffHeapSkipList skipList;
    private final WAL wal;
    private final String walFileName;

    // 物理阈值：通常设为 Arena 总容量的 90% 或 95%，预留缓冲空间防止最后一条大记录越界
    private final int memoryThreshold;

    // 状态机事实：利用 CAS 原子布尔值，确保只有一次冻结操作
    private final AtomicBoolean isImmutable = new AtomicBoolean(false);

    /**
     * @param capacity     Arena 分配的堆外内存总大小
     * @param walDirectory WAL 日志文件目录
     * @param walFileName  WAL 文件名
     */
    public MemTable(int capacity, String walDirectory, String walFileName) throws IOException {
        this.arena = new Arena(capacity);
        this.skipList = new OffHeapSkipList(this.arena);
        this.wal = new WAL(walDirectory, walFileName);
        this.walFileName = walFileName;

        // 事实：设定 90% 为安全水位线
        this.memoryThreshold = (int) (capacity * 0.90);
    }

    /**
     * 不仅返回 Value，还要返回包含 Pointer 的完整记录
     */
    public LogRecord getRecord(byte[] key) {
        return skipList.getRecord(key);
    }

    /**
     * 对外暴露有序数据流，专供 Flush 到 SSTable 使用
     */
    @Override
    public Iterator<LogRecord> iterator() {
        // 状态机防御事实：通常只允许对 Immutable 的 MemTable 进行迭代刷盘
        if (!isImmutable.get()) {
            throw new IllegalStateException("Cannot iterate a mutable MemTable. Flush is only allowed for Immutable MemTable.");
        }

        // 直接委托给底层跳表
        return skipList.iterator();
    }

    public boolean put(byte type, byte[] key, byte[]value, byte[]pointerBytes){
        if(isImmutable.get()){
            return false;
        }
        LogRecord record = new LogRecord(type, key, value, pointerBytes);
        try{
            wal.append(LogRecordCoder.encode(record));
            skipList.put(record);
            checkMemoryUsage();
            return true;
        }catch (Exception e){
            System.err.println("MemTable 统一写入失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 逻辑删除链路 (追加墓碑)
     */
    public boolean delete(byte[] key) {
        if (isImmutable.get()) {
            return false;
        }

        // 构造墓碑节点
        LogRecord tombstone = new LogRecord(LogRecordType.DELETE, key, new byte[0],null);
        ByteBuffer encodedData = LogRecordCoder.encode(tombstone);

        // 严格遵循 WAL -> SkipList 时序
        wal.append(encodedData);
        skipList.put(tombstone); // 这里实际上我们在 SkipList 中无需特殊处理，当做普通 put 即可，由于你的左侧插入机制，墓碑会挡在旧数据前面

        checkMemoryUsage();
        return true;
    }

    /**
     * 将 BlobWriter 的指挥权交给内部的 WAL
     */
    public void setBlobWriter(BlobWriter blobWriter) {
        if (this.wal != null) {
            this.wal.setBlobWriter(blobWriter);
        }
    }

    public String getWalFileName() {
        return this.walFileName;
    }

    /**
     * 用于引擎启动时的 WAL 宕机恢复。
     * 不能在这个方法里调用 wal.append()，否则会导致日志双写灾难
     */
    public void restoreFromWal(LogRecord record) {
        skipList.put(record);
        // 2. 检查水位线，防备断电前遗留的数据量极大，直接把 MemTable 撑满了
        checkMemoryUsage();
    }

    /**
     * 暴露给上层引擎的状态查询
     */
    public boolean isImmutable() {
        return isImmutable.get();
    }

    /**
     * 暴露给上层引擎的强制冻结操作
     * 事实：由 CarinaEngine 的写锁保证并发安全，此处直接 set 即可
     */
    public void freeze() {
        isImmutable.set(true);
    }

    /**
     * 生命周期管理：关闭资源
     */
    public void close() throws IOException {
        wal.close();
        // 注意：Arena 使用的是堆外直接内存，在 Java 中依赖 Cleaner 回收，
        // 或者在此处通过 Unsafe 手动调用 freeMemory 释放。V1 版本可先依赖系统回收。
    }

    /**
     * 水位越线检测与状态翻转
     */
    private void checkMemoryUsage() {
        if (arena.memoryUsage() >= memoryThreshold) {
            // 物理事实：利用 CAS 操作，确保在并发环境下，只会有 1 个线程成功执行翻转
            isImmutable.compareAndSet(false, true);
        }
    }
}