package com.jiashi.db.engine.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 核心读链路：负责解析 SSTable 文件，加载内存目录，并提供点查能力
 */
public class SSTableReader {

    private final FileChannel channel;
    // 内存目录：存放 (最大Key -> DataBlock物理偏移量) 的映射
    private final List<IndexEntry> indexEntries = new ArrayList<>();

    // 内部类：表示目录中的一行记录
    private static class IndexEntry {
        byte[] maxKey;
        long blockOffset;

        IndexEntry(byte[] maxKey, long blockOffset) {
            this.maxKey = maxKey;
            this.blockOffset = blockOffset;
        }
    }

    public SSTableReader(Path filePath) throws IOException {
        // 以纯读模式打开文件通道
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        long fileSize = channel.size();

        // ---------------------------------------------------------
        // 步骤 1：精确空降到文件绝对末尾，读取 24 字节 Footer
        // ---------------------------------------------------------
        ByteBuffer footerBuffer = ByteBuffer.allocate(Footer.ENCODED_LENGTH);
        channel.position(fileSize - Footer.ENCODED_LENGTH);
        channel.read(footerBuffer);
        footerBuffer.flip(); // 切换为读模式以提取数据

        long metaIndexOffset = footerBuffer.getLong();
        long dataIndexOffset = footerBuffer.getLong();
        long magicNumber = footerBuffer.getLong();

        // 【安全防线】如果魔数对不上，说明文件损坏或根本不是 carinaDB 的文件
        if (magicNumber != Footer.MAGIC_NUMBER) {
            throw new IllegalStateException("SSTable 魔数校验失败，文件损坏或格式错误！");
        }

        // ---------------------------------------------------------
        // 步骤 2：根据解析出的偏移量，将整个 Index Block 读入内存
        // ---------------------------------------------------------
        // Index Block 的大小 = 文件总长 - Footer长度 - Index的起始位置
        int indexBlockSize = (int) (fileSize - Footer.ENCODED_LENGTH - dataIndexOffset);
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexBlockSize);
        channel.position(dataIndexOffset);
        channel.read(indexBuffer);
        indexBuffer.flip();

        // ---------------------------------------------------------
        // 步骤 3：反序列化 Index Block，在内存中建立 List 目录
        // ---------------------------------------------------------
        while (indexBuffer.hasRemaining()) {
            int keyLen = indexBuffer.getInt();
            byte[] key = new byte[keyLen];
            indexBuffer.get(key);

            int valLen = indexBuffer.getInt(); // 实际上就是 8
            byte[] val = new byte[valLen];
            indexBuffer.get(val);

            // 将 8 字节的 value 还原成 long 类型的偏移量
            long blockOffset = ByteBuffer.wrap(val).getLong();
            indexEntries.add(new IndexEntry(key, blockOffset));
        }
    }

    /**
     * 核心点查方法：寻找指定的 Key (使用二分查找定位 Data Block)
     * @return 如果找到，返回对应的 Value (标量+VectorID)；否则返回 null
     */
    public byte[] searchBinary(byte[] targetKey) throws IOException {
        // 1. 在内存目录中进行二分查找 (寻找 Lower Bound)
        long targetBlockOffset = -1;
        long nextBlockOffset = -1;
        int targetIndex = -1;

        int low = 0;
        int high = indexEntries.size() - 1;

        while (low <= high) {
            // 无符号右移，防止位溢出，标准二分写法
            int mid = (low + high) >>> 1;
            IndexEntry midEntry = indexEntries.get(mid);

            // Arrays.compare 结果：<0 表示 targetKey 小，==0 表示相等，>0 表示 targetKey 大
            int cmp = compareBytes(targetKey, midEntry.maxKey);

            if (cmp == 0) {
                // 精准命中某个块的 maxKey
                targetIndex = mid;
                break;
            } else if (cmp < 0) {
                // targetKey < maxKey，说明目标可能在这个块里，也可能在更前面的块里
                targetIndex = mid; // 暂存当前块为候选块
                high = mid - 1;    // 继续向左半区收缩范围
            } else {
                // targetKey > maxKey，说明目标绝对不可能在这个块及之前的块里
                low = mid + 1;     // 向右半区寻找
            }
        }

        // 解析出需要的物理坐标
        if (targetIndex != -1) {
            targetBlockOffset = indexEntries.get(targetIndex).blockOffset;
            if (targetIndex + 1 < indexEntries.size()) {
                nextBlockOffset = indexEntries.get(targetIndex + 1).blockOffset;
            }
        } else {
            return null; // targetKey 比目录里所有的 maxKey 都要大，绝对不存在
        }

        // 2. 发起一次精准的磁盘 I/O，将目标 Data Block 读入内存
        int blockSize;
        if (nextBlockOffset != -1) {
            blockSize = (int) (nextBlockOffset - targetBlockOffset);
        } else {
            // 最后一个块，直接读足够大的空间，或者精准读取剩余的文件大小
            blockSize = 4096 * 2;
        }

        ByteBuffer dataBuffer = ByteBuffer.allocate(blockSize);
        channel.position(targetBlockOffset);
        channel.read(dataBuffer);
        dataBuffer.flip();

        // 3. 在内存块中进行线性扫描反序列化，精确定位 KV
        // (注：工业界在此处通常也会将 Block 内部的数据格式设计为支持二分查找，目前暂保留线性扫描)
        while (dataBuffer.hasRemaining()) {
            int keyLen = dataBuffer.getInt();
            if (keyLen <= 0) break; // 预防空块或越界

            byte[] currentKey = new byte[keyLen];
            dataBuffer.get(currentKey);

            int valLen = dataBuffer.getInt();
            byte[] currentValue = new byte[valLen];
            dataBuffer.get(currentValue);

            // 精准匹配
            if (Arrays.equals(targetKey, currentKey)) {
                return currentValue;
            }
        }

        return null;
    }

    /**
     * 核心点查方法：寻找指定的 Key
     * @return 如果找到，返回对应的 Value (标量+VectorID)；否则返回 null
     */
    public byte[] searchLinear(byte[] targetKey) throws IOException {
        // 1. 在内存目录中进行遍历/二分查找，锁定目标 Data Block
        // 由于是 MVP，这里使用简单的遍历寻找第一个大于等于 targetKey 的 IndexEntry
        long targetBlockOffset = -1;
        long nextBlockOffset = -1;

        for (int i = 0; i < indexEntries.size(); i++) {
            IndexEntry entry = indexEntries.get(i);
            // Arrays.compare(a, b) 如果 a <= b 返回 <= 0
            if (compareBytes(targetKey, entry.maxKey) <= 0) {
                targetBlockOffset = entry.blockOffset;
                // 推算该 Block 的大小，用于精准读取
                if (i + 1 < indexEntries.size()) {
                    nextBlockOffset = indexEntries.get(i + 1).blockOffset;
                }
                break;
            }
        }

        if (targetBlockOffset == -1) {
            return null; // 目录中所有最大 Key 都比目标小，绝对不存在
        }

        // 2. 发起一次精准的磁盘 I/O，将目标 Data Block 读入内存
        // 如果是最后一个块，它的大小就是 Index Block 的起始位置减去它的 Offset
        int blockSize;
        if (nextBlockOffset != -1) {
            blockSize = (int) (nextBlockOffset - targetBlockOffset);
        } else {
            // 需要再次查底层文件大小来推算最后一个块的边界 (这里简化逻辑，通常 Block 固定上限是 4KB+)
            blockSize = 4096 * 2; // 预留足够空间
        }

        ByteBuffer dataBuffer = ByteBuffer.allocate(blockSize);
        channel.position(targetBlockOffset);
        channel.read(dataBuffer);
        dataBuffer.flip();

        // 3. 在内存块中进行线性扫描反序列化，精确定位 KV
        while (dataBuffer.hasRemaining()) {
            int keyLen = dataBuffer.getInt();
            if (keyLen <= 0) break; // 预防空块或越界

            byte[] currentKey = new byte[keyLen];
            dataBuffer.get(currentKey);

            int valLen = dataBuffer.getInt();
            byte[] currentValue = new byte[valLen];
            dataBuffer.get(currentValue);

            // 精准匹配
            if (Arrays.equals(targetKey, currentKey)) {
                return currentValue;
            }
        }

        // 发生了布隆过滤器假阳性(如果未来接入) 或 Index 命中但块内没有
        return null;
    }

    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len1 = a.length;
        int len2 = b.length;
        int lim = Math.min(len1, len2);
        for (int i = 0; i < lim; i++) {
            // 关键逻辑：& 0xFF 将有符号 byte 转换为无符号的 int 进行真实比较
            int c1 = a[i] & 0xFF;
            int c2 = b[i] & 0xFF;
            if (c1 != c2) {
                return c1 - c2;
            }
        }
        // 前缀相同的情况下，长度更长的字节数组更大
        return len1 - len2;
    }
}