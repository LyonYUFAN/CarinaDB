package com.jiashi.db.engine.memtable;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.common.model.LogRecordType;
import com.jiashi.db.common.coder.LogRecordCoder; // 假设你有这个编码器
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

        // 事实：设定 90% 为安全水位线
        this.memoryThreshold = (int) (capacity * 0.90);
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

    /**
     * 对外暴露的写入链路
     * @return true 表示写入成功；false 表示 MemTable 已满（需要上层切换）
     */
    public boolean put(byte[] key, byte[] value) {
        // 1. 状态机防御：如果已经冻结，直接拒绝写入
        if (isImmutable.get()) {
            return false;
        }

        // 构造逻辑载体
        LogRecord record = new LogRecord(LogRecordType.PUT_KV, key, value);

        // 2. 物理落盘链路 (严格前置)
        // 事实：WAL 只能接收纯字节，必须先将 Record 序列化
        ByteBuffer encodedData = LogRecordCoder.encode(record);

        // 此处会进入组提交并发队列，阻塞直至强制物理落盘 (fsync)
        wal.append(encodedData);

        // 3. 内存索引挂载 (确保日志绝对安全后执行)
        skipList.put(record);

        // 4. 水位检测：每次写完检查物理游标是否越线
        checkMemoryUsage();

        return true;
    }

    /**
     * 对外暴露的向量写入链路 (Vector 专属)
     */
    public boolean putVector(byte[] key, byte[] value, float[] vector) {
        // 1. 状态机防御
        if (isImmutable.get()) {
            return false;
        }

        // 2. 物理事实：明确使用 PUT_VECTOR 类型，并挂载向量数组
        LogRecord record = new LogRecord(LogRecordType.PUT_VECTOR, key, value, vector);

        // 3. 严格遵循 WAL -> SkipList 的写入时序
        ByteBuffer encodedData = LogRecordCoder.encode(record);
        wal.append(encodedData);
        skipList.put(record);

        // 4. 水位检测
        checkMemoryUsage();

        return true;
    }

    /**
     * 极速无锁读取链路
     */
    public byte[] get(byte[] key) {
        // 事实：直接委托给底层的 O(log n) 物理寻路器
        return skipList.get(key);
    }

    /**
     * 逻辑删除链路 (追加墓碑)
     */
    public boolean delete(byte[] key) {
        if (isImmutable.get()) {
            return false;
        }

        // 构造墓碑节点
        LogRecord tombstone = new LogRecord(LogRecordType.DELETE, key, new byte[0]);
        ByteBuffer encodedData = LogRecordCoder.encode(tombstone);

        // 严格遵循 WAL -> SkipList 时序
        wal.append(encodedData);
        skipList.put(tombstone); // 这里实际上我们在 SkipList 中无需特殊处理，当做普通 put 即可，由于你的左侧插入机制，墓碑会挡在旧数据前面

        checkMemoryUsage();
        return true;
    }

    /**
     * 内部辅助：水位越线检测与状态翻转
     */
    private void checkMemoryUsage() {
        if (arena.memoryUsage() >= memoryThreshold) {
            // 物理事实：利用 CAS 操作，确保在并发环境下，只会有 1 个线程成功执行翻转
            isImmutable.compareAndSet(false, true);
        }
    }

    /**
     * 暴露给上层引擎的状态查询
     */
    public boolean isImmutable() {
        return isImmutable.get();
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
     * 暴露给上层引擎的强制冻结操作
     * 事实：由 CarinaEngine 的写锁保证并发安全，此处直接 set 即可
     */
    public void freeze() {
        isImmutable.set(true);
    }
}