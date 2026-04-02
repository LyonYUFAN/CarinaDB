package com.jiashi.db.engine.sstable;

import java.nio.ByteBuffer;

/**
 * 负责在内存中构建4KB的数据块
 * 既可以用来构建Data Block，也可以用来构建Index Block
 */
public class BlockBuilder {
    // 默认块大小 4KB
    private static final int BLOCK_SIZE = 4096;

    // 使用堆外内存，提升落盘速度
    private ByteBuffer buffer;

    public BlockBuilder() {
        this.buffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
    }

    /**
     * 尝试将KV写入当前块。
     * 物理结构: Key_Length(4) + Key + Value_Length(4) + Value
     * * @return 如果当前块满了，返回 false，提示上层需要刷盘了；写入成功返回 true
     */
    public boolean add(byte[] key, byte[] value) {
        // 计算这条记录需要的物理空间
        int requiredSpace = Integer.BYTES * 2 + key.length + value.length;

        // 判断是否超过 4KB 边界 (这里允许最后一条记录轻微溢出，为了简化设计)
        if (buffer.position() > 0 && buffer.position() + requiredSpace > buffer.capacity()) {
            // 当前块已满，不能再加了
            return false;
        }

        // 如果容量不够但这是块里的第一条记录，我们需要扩容 (防止单条 KV 本身就大于 4KB)
        if (buffer.capacity() - buffer.position() < requiredSpace) {
            ByteBuffer newBuffer = ByteBuffer.allocateDirect(buffer.position() + requiredSpace);
            buffer.flip();
            newBuffer.put(buffer);
            this.buffer = newBuffer;
        }

        // 写入字节流
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);

        return true;
    }

    /**
     * 封块：将ByteBuffer翻转为可读模式，并返回给上层准备写入FileChannel
     */
    public ByteBuffer finish() {
        buffer.flip();
        return buffer;
    }

    /**
     * 重置Builder，以便复用内存结构构建下一个Block
     */
    public void reset() {
        buffer.clear();
    }

    public boolean isEmpty() {
        return buffer.position() == 0;
    }
}