package com.jiashi.db.common.coder;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.common.model.LogRecordType;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 负责LogRecord与字节流的互相转换
 */
public class LogRecordCoder {

    // Header 的固定物理长度：CRC(4) + RecordSize(4)
    public static final int HEADER_SIZE = 8;

    /**
     * 编码：将对象转换为可直接刷入磁盘的 ByteBuffer
     */
    public static ByteBuffer encode(LogRecord record) {
        // 1. 预计算Payload的物理长度 (Type + KeySize + Key + ValueSize + Value)
        int payloadSize = 1 + 4 + record.getKey().length + 4 + record.getValue().length;

        // 如果是向量类型，追加维度(4)和实际float数组的字节数
        if (record.getType() == LogRecordType.PUT_VECTOR && record.getVector() != null) {
            payloadSize += 4 + (record.getVector().length * 4);
        }

        // 2. 一次性分配整条数据所需的内存(Header + Payload)
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payloadSize);

        // --- 开始组装Payload(跳过前8个字节的Header区) ---
        buffer.position(HEADER_SIZE);

        buffer.put(record.getType());
        buffer.putInt(record.getKey().length);
        buffer.put(record.getKey());
        buffer.putInt(record.getValue().length);
        buffer.put(record.getValue());

        if (record.getType() == LogRecordType.PUT_VECTOR && record.getVector() != null) {
            buffer.putInt(record.getVector().length);
            for (float f : record.getVector()) {
                buffer.putFloat(f);
            }
        }

        // 3. 计算Payload的CRC32数字指纹
        CRC32 crc32 = new CRC32();
        // array() 获取底层 byte[]，从第8个字节开始算，长度为payloadSize
        crc32.update(buffer.array(), HEADER_SIZE, payloadSize);
        int crc = (int) crc32.getValue();

        // 4. 回到最开头，填入定长 Header
        buffer.position(0);
        buffer.putInt(crc);
        buffer.putInt(payloadSize);

        // 5. 将 ByteBuffer 重置为读模式，准备交给 FileChannel 写入磁盘
        buffer.position(0);
        return buffer;
    }

    /**
     * 解码：在系统重启时，把校验通过的Payload字节流还原为对象
     * 注意：传入的ByteBuffer已经是剥离了Header并通过了CRC校验的纯Payload数据
     */
    public static LogRecord decodePayload(ByteBuffer payloadBuffer) {
        byte type = payloadBuffer.get();

        int keySize = payloadBuffer.getInt();
        byte[] key = new byte[keySize];
        payloadBuffer.get(key);

        int valueSize = payloadBuffer.getInt();
        byte[] value = new byte[valueSize];
        payloadBuffer.get(value);

        float[] vector = null;
        if (type == LogRecordType.PUT_VECTOR) {
            int dim = payloadBuffer.getInt();
            vector = new float[dim];
            for (int i = 0; i < dim; i++) {
                vector[i] = payloadBuffer.getFloat();
            }
        }
        return new LogRecord(type, key, value, vector);
    }
}