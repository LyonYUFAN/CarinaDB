package com.jiashi.db.common.coder;

import com.jiashi.db.common.model.LogRecord;
import sun.security.krb5.internal.crypto.crc32;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 负责LogRecord与字节流的互相转换
 */
public class LogRecordCoder {

    // Header 的固定物理长度：CRC(4) + RecordSize(4)
    public static final int HEADER_SIZE = 8;

    public static ByteBuffer encode(LogRecord record){
        byte[] key = record.getKey();
        byte[] value = record.getValue();
        byte[] pointer = record.getBlobPointer();
        int keyLen = key != null ? key.length : 0;
        int valLen = value != null ? value.length : 0;
        int ptrLen = pointer != null ? pointer.length : 0;
        // 固定部分: Type(1) + KeyLen(4) + ValLen(4) + PtrLen(4) = 13字节
        // 变长部分: keyLen + valLen + ptrLen
        int payloadSize = 1 + 4 + 4 + 4 + keyLen + valLen + ptrLen;
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payloadSize);

        buffer.position(HEADER_SIZE);

        buffer.put(record.getType());
        buffer.putInt(keyLen);
        buffer.putInt(valLen);
        buffer.putInt(ptrLen);

        if (keyLen > 0) buffer.put(key);
        if (valLen > 0) buffer.put(value);
        if (ptrLen > 0) buffer.put(pointer);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), HEADER_SIZE, payloadSize);
        int crc = (int) crc32.getValue();

        buffer.position(0);
        buffer.putInt(crc);
        buffer.putInt(payloadSize);

        buffer.position(0);
        return buffer;
    }

    /**
     * 解码：在系统重启时，把校验通过的 Payload 字节流还原为对象
     */
    public static LogRecord decodePayload(ByteBuffer payloadBuffer) {
        // ✍️ 严格按照编码顺序解箱
        byte type = payloadBuffer.get();
        int keyLen = payloadBuffer.getInt();
        int valLen = payloadBuffer.getInt();
        int ptrLen = payloadBuffer.getInt();

        byte[] key = null;
        if (keyLen > 0) {
            key = new byte[keyLen];
            payloadBuffer.get(key);
        }

        byte[] value = null;
        if (valLen > 0) {
            value = new byte[valLen];
            payloadBuffer.get(value);
        }

        byte[] pointer = null;
        if (ptrLen > 0) {
            pointer = new byte[ptrLen];
            payloadBuffer.get(pointer);
        }

        // 组装并返回 V3.0 的 LogRecord
        return new LogRecord(type, key, value, pointer);
    }
}