package com.jiashi.db.common.model;

/**
 * 内存中的数据载体，不涉及底层字节逻辑
 */
public class LogRecord {
    private byte type;
    private byte[] key;
    private byte[] value;
    private float[] vector; // 仅在 type 为 PUT_VECTOR 时有值

    // 针对纯 KV 或 Delete 的构造器
    public LogRecord(byte type, byte[] key, byte[] value) {
        this(type, key, value, null);
    }

    // 针对包含向量的完整构造器
    public LogRecord(byte type, byte[] key, byte[] value, float[] vector) {
        this.type = type;
        this.key = key != null ? key : new byte[0];
        this.value = value != null ? value : new byte[0];
        this.vector = vector;
    }

    public byte getType() { return type; }
    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public float[] getVector() { return vector; }
}