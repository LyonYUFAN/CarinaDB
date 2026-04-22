package com.jiashi.db.common.model;

/**
 * 内存中的数据载体，不涉及底层字节逻辑
 */
// TODO(MVP-Debt): 缺乏 MVCC 全局序列号 (Sequence Number)。
public class LogRecord {
    private byte type;
    private byte[] key;
    private byte[] value;

    private final byte[] blobPointer;

    public LogRecord(byte type, byte[] key, byte[] value, byte[] blobPointer) {
        this.type = type;
        this.key = key != null ? key : new byte[0];
        this.value = value != null ? value : new byte[0];
        this.blobPointer = blobPointer;
    }

    public byte getType() { return type; }
    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public byte[] getBlobPointer() { return blobPointer; }
}