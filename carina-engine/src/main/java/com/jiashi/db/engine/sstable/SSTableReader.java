package com.jiashi.db.engine.sstable;

import com.jiashi.db.common.filter.BloomFilter;
import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.engine.cache.BlockCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 核心读链路：集成三级 LRU 缓存的按需加载（Lazy Loading）实现
 */
public class SSTableReader {

    // ================= 全局内存大管家 (防止 OOM) =================
    // 物理事实：这里定义为 static，意味着整个引擎里所有的 SSTable 共享这些容量配额
    // 1. 数据块缓存：假设缓存 1000 个 4KB Data Block (~4MB)
    private static final BlockCache<String, byte[]> DATA_BLOCK_CACHE = new BlockCache<>(1000);
    // 2. 过滤器缓存：假设缓存 100 个 SSTable 的布隆过滤器
    private static final BlockCache<String, BloomFilter> FILTER_CACHE = new BlockCache<>(100);
    // 3. 目录块缓存：假设缓存 100 个 SSTable 的 Index Block 结构
    private static final BlockCache<String, List<IndexEntry>> INDEX_CACHE = new BlockCache<>(100);

    private final String fileId;
    private final FileChannel channel;

    // ======== 极小元数据：开机常驻内存，用于全局极值地图筛选 ========
    private final byte[] minKey;
    private final byte[] maxKey;

    // ======== 物理指针：仅记录坐标，等待按需加载 ========
    private final long filterOffset;
    private final long indexBlockOffset;
    private final long minKeyOffset;
    private final long maxKeyOffset;

    public static class IndexEntry {
        public final byte[] maxKey;
        public final long blockOffset;

        IndexEntry(byte[] maxKey, long blockOffset) {
            this.maxKey = maxKey;
            this.blockOffset = blockOffset;
        }
    }

    /**
     * 极速启动构造函数：严格遵循“只读尾部极小数据”的物理事实
     */
    public SSTableReader(Path filePath) throws IOException {
        this.fileId = filePath.getFileName().toString();
        // 事实：纯读模式打开，防止误修改落盘的历史文件
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        long fileSize = channel.size();

        // ---------------------------------------------------------
        // 步骤 1：空降文件绝对末尾，只读 40 字节 Footer
        // ---------------------------------------------------------
        ByteBuffer footerBuffer = ByteBuffer.allocate(Footer.ENCODED_LENGTH);
        channel.position(fileSize - Footer.ENCODED_LENGTH);
        channel.read(footerBuffer);
        footerBuffer.flip();

        this.filterOffset = footerBuffer.getLong();
        this.indexBlockOffset = footerBuffer.getLong();
        this.minKeyOffset = footerBuffer.getLong();
        this.maxKeyOffset = footerBuffer.getLong();
        long magicNumber = footerBuffer.getLong();

        if (magicNumber != Footer.MAGIC_NUMBER) {
            throw new IllegalStateException("SSTable 魔数校验失败，文件损坏或非 CarinaDB 文件！");
        }

        // ---------------------------------------------------------
        // 步骤 2：顺藤摸瓜，从指针位置读取具体的 MinKey 和 MaxKey
        // ---------------------------------------------------------
        this.minKey = readKeyWithLength(this.minKeyOffset);
        this.maxKey = readKeyWithLength(this.maxKeyOffset);
    }

    public byte[] getMinKey() { return minKey; }
    public byte[] getMaxKey() { return maxKey; }
    public String getFileId() { return fileId; }

    /**
     * WiscKey 分离读取
     */
    public LogRecord searchBinaryFullRecord(byte[] targetKey) throws IOException {
        BloomFilter filter = getBloomFilterLazy();
        if (!filter.mightContain(targetKey)) return null;
        List<IndexEntry> indexEntries = getIndexBlockLazy();
        long targetBlockOffset = -1;
        long nextBlockOffset = -1;
        int low = 0, high = indexEntries.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry midEntry = indexEntries.get(mid);
            int cmp = compareBytes(targetKey, midEntry.maxKey);
            if (cmp == 0) {
                targetBlockOffset = midEntry.blockOffset;
                if (mid + 1 < indexEntries.size()) nextBlockOffset = indexEntries.get(mid + 1).blockOffset;
                break;
            } else if (cmp < 0) {
                targetBlockOffset = midEntry.blockOffset;
                if (mid + 1 < indexEntries.size()) nextBlockOffset = indexEntries.get(mid + 1).blockOffset;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
            if (targetBlockOffset == -1) return null;
            byte[] dataBlock = getDataBlockLazy(targetBlockOffset, nextBlockOffset);
            ByteBuffer blockReader = ByteBuffer.wrap(dataBlock);
            while(blockReader.hasRemaining()){
                byte type = blockReader.get();
                int keyLen = blockReader.getInt();
                if (keyLen <= 0) break;
                int valLen = blockReader.getInt();
                int pointerLen = blockReader.getInt();
                byte[] currentKey = new byte[keyLen];
                blockReader.get(currentKey);

                byte[] currentValue = new byte[valLen];
                blockReader.get(currentValue);

                byte[] currentPointer = null;
                if (pointerLen > 0) {
                    currentPointer = new byte[pointerLen];
                    blockReader.get(currentPointer);
                }
                if (Arrays.equals(targetKey, currentKey)) {
                    return new LogRecord(type, currentKey, currentValue, currentPointer);
                }
            }
            return null;
    }

    // ------------------------------------------------------------------------
    // 三大核心按需懒加载修复：强制安全读取 (Safe Block Read)
    // ------------------------------------------------------------------------

    private BloomFilter getBloomFilterLazy() throws IOException {
        BloomFilter filter = FILTER_CACHE.get(fileId);
        if (filter == null) {
            int filterSize = (int) (indexBlockOffset - filterOffset);
            ByteBuffer filterBuffer = ByteBuffer.allocate(filterSize);
            channel.position(filterOffset);

            while (filterBuffer.hasRemaining()) {
                if (channel.read(filterBuffer) == -1) break;
            }

            // 【关键修复】翻转游标，只拿取真正读到的干净字节
            filterBuffer.flip();
            byte[] exactBytes = new byte[filterBuffer.remaining()];
            filterBuffer.get(exactBytes);

            filter = new BloomFilter(exactBytes);
            FILTER_CACHE.put(fileId, filter);
        }
        return filter;
    }

    private List<IndexEntry> getIndexBlockLazy() throws IOException {
        List<IndexEntry> indexEntries = INDEX_CACHE.get(fileId);
        if (indexEntries == null) {
            int indexSize = (int) (minKeyOffset - indexBlockOffset);
            ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
            channel.position(indexBlockOffset);

            while (indexBuffer.hasRemaining()) {
                if (channel.read(indexBuffer) == -1) break;
            }
            indexBuffer.flip();

            indexEntries = new ArrayList<>();
            while (indexBuffer.hasRemaining()) {
                int keyLen = indexBuffer.getInt();
                byte[] key = new byte[keyLen];
                indexBuffer.get(key);
                int valLen = indexBuffer.getInt(); // 8
                byte[] val = new byte[valLen];
                indexBuffer.get(val);
                long blockOffset = ByteBuffer.wrap(val).getLong();
                indexEntries.add(new IndexEntry(key, blockOffset));
            }
            INDEX_CACHE.put(fileId, indexEntries);
        }
        return indexEntries;
    }

    private byte[] getDataBlockLazy(long targetBlockOffset, long nextBlockOffset) throws IOException {
        String cacheKey = fileId + "_data_" + targetBlockOffset;
        byte[] dataBlock = DATA_BLOCK_CACHE.get(cacheKey);

        if (dataBlock == null) {
            int blockSize = (nextBlockOffset != -1) ?
                    (int) (nextBlockOffset - targetBlockOffset) :
                    (int) (filterOffset - targetBlockOffset);

            ByteBuffer dataBuffer = ByteBuffer.allocate(blockSize);
            channel.position(targetBlockOffset);

            while (dataBuffer.hasRemaining()) {
                if (channel.read(dataBuffer) == -1) break;
            }

            // 【关键修复】翻转并提取干净数据，防止末尾残留 0 导致后续读块死循环
            dataBuffer.flip();
            dataBlock = new byte[dataBuffer.remaining()];
            dataBuffer.get(dataBlock);

            DATA_BLOCK_CACHE.put(cacheKey, dataBlock);
        }
        return dataBlock;
    }

    // ------------------------------------------------------------------------
    // 物理层辅助工具
    // ------------------------------------------------------------------------

    private byte[] readKeyWithLength(long offset) throws IOException {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        channel.position(offset);
        while (lenBuf.hasRemaining()) {
            if (channel.read(lenBuf) == -1) break;
        }
        lenBuf.flip();
        int len = lenBuf.getInt();

        ByteBuffer keyBuf = ByteBuffer.allocate(len);
        while (keyBuf.hasRemaining()) {
            if (channel.read(keyBuf) == -1) break;
        }
        keyBuf.flip();
        return keyBuf.array();
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len1 = a.length;
        int len2 = b.length;
        int lim = Math.min(len1, len2);
        for (int i = 0; i < lim; i++) {
            // 转为无符号正数比对
            int c1 = a[i] & 0xFF;
            int c2 = b[i] & 0xFF;
            if (c1 != c2) { return c1 - c2; }
        }
        return len1 - len2;
    }

    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }
}