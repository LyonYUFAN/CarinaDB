package com.jiashi.db.engine.blob;

import java.nio.ByteBuffer;

/**
 * 向量数据的物理寻址指针 (仅占 20 字节)
 */
public class BlobPointer {
    public final long fileId;
    public final long offset;
    public final int vectorDim;

    public BlobPointer(long fileId, long offset, int vectorDim) {
        this.fileId = fileId;
        this.offset = offset;
        this.vectorDim = vectorDim;
    }

    // 将指针序列化为 20 字节的数组，用于伪装成普通的 Value 塞进 LSM-Tree
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putLong(fileId);
        buffer.putLong(offset);
        buffer.putInt(vectorDim);
        return buffer.array();
    }

    // 从 20 字节的数组还原出物理指针
    public static BlobPointer fromBytes(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new BlobPointer(buffer.getLong(), buffer.getLong(), buffer.getInt());
    }
}