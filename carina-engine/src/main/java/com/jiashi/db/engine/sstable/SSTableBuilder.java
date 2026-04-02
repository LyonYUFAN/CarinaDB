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
 * 将 MemTable 中的数据流式转换为磁盘上的 SSTable 文件
 */
public class SSTableBuilder {

    private final FileChannel channel;
    private final BlockBuilder dataBlockBuilder;

    // 修复点：使用 List 在内存中无限量暂存 Index 条目，彻底摆脱 4KB 限制
    private final List<IndexEntry> memIndex = new ArrayList<>();

    private long currentOffset = 0;
    private byte[] lastKey;

    // 内部类暂存目录
    private static class IndexEntry {
        byte[] key;
        long blockOffset;
        IndexEntry(byte[] key, long blockOffset) {
            this.key = key;
            this.blockOffset = blockOffset;
        }
    }

    public SSTableBuilder(Path filePath) throws IOException {
        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        this.dataBlockBuilder = new BlockBuilder();
    }

    public void add(byte[] key, byte[] value) throws IOException {
        if (!dataBlockBuilder.add(key, value)) {
            flushDataBlock();
            dataBlockBuilder.add(key, value);
        }
        this.lastKey = Arrays.copyOf(key, key.length);
    }

    private void flushDataBlock() throws IOException {
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        ByteBuffer dataBuffer = dataBlockBuilder.finish();

        // 【核心修复】将最大的 Key 和当前的物理 Offset 记录到内存列表中，永不丢失
        memIndex.add(new IndexEntry(lastKey, currentOffset));

        int written = 0;
        while (dataBuffer.hasRemaining()) {
            written += channel.write(dataBuffer);
        }

        currentOffset += written;
        dataBlockBuilder.reset();
    }

    public void finish() throws IOException {
        flushDataBlock();

        long indexBlockOffset = currentOffset;

        // 1. 精确计算百万级 Index Block 的总字节大小
        int indexSize = 0;
        for (IndexEntry entry : memIndex) {
            indexSize += Integer.BYTES + entry.key.length + Integer.BYTES + Long.BYTES;
        }

        // 2. 一次性分配足够大的堆外内存，将整个超级大目录拼装好
        ByteBuffer indexBuffer = ByteBuffer.allocateDirect(indexSize);
        for (IndexEntry entry : memIndex) {
            indexBuffer.putInt(entry.key.length);
            indexBuffer.put(entry.key);
            indexBuffer.putInt(8); // Offset 是 long 型，固定 8 字节
            indexBuffer.putLong(entry.blockOffset);
        }
        indexBuffer.flip();

        // 3. 一发入魂，写入磁盘
        while (indexBuffer.hasRemaining()) {
            currentOffset += channel.write(indexBuffer);
        }

        long metaIndexOffset = 0;
        Footer.writeFooter(channel, metaIndexOffset, indexBlockOffset);

        channel.force(true);
        channel.close();
    }
}