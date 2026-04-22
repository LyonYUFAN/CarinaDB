package com.jiashi.db.engine.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 负责在内存中构建4KB的数据块
 * 既可以用来构建 Data Block，也可以用来构建 Index Block
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
     * 核心写块链路
     * @return true 写入成功；false 表示当前 4KB 块已满，需要上层换新块
     */
    public boolean add(byte[] key, byte[] value, byte type, byte[] pointer) throws IOException {
        int keyLen = key !=null ? key.length : 0;
        int valLen = value != null ? value.length : 0;
        int ptrLen = pointer !=null ? pointer.length : 0;

        int requiredSpace = 13 + keyLen + valLen + ptrLen;
        if (buffer.remaining() < requiredSpace) {
            return false;
        }
        buffer.put(type);
        buffer.putInt(keyLen);
        buffer.putInt(valLen);
        buffer.putInt(ptrLen);

        if (keyLen > 0) buffer.put(key);
        if (valLen > 0) buffer.put(value);
        if (ptrLen > 0) buffer.put(pointer);
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