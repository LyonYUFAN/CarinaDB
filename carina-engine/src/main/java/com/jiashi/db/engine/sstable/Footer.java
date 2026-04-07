package com.jiashi.db.engine.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * SSTable 的定长封底 (Fixed Footer)
 * 物理布局事实：严格固定为 40 字节，永远处于 .sst 文件的绝对末尾。
 * 作用：作为文件反向解析的唯一入口路标，彻底解耦数据区与元数据区的物理耦合。
 */
public class Footer {

    // CarinaDB 专属魔数 (Hex 编码的 "CarinaDB")
    // 作用：防线 0，确保系统不会误读其他格式的损坏文件
    public static final long MAGIC_NUMBER = 0x436172696E614442L;

    // 物理事实：5 个 long 型变量 (5 * 8 = 40 字节)
    // 布局顺序：filterOffset -> indexOffset -> minKeyOffset -> maxKeyOffset -> magicNumber
    public static final int ENCODED_LENGTH = 40;

    /**
     * 供写链路 (SSTableBuilder) 调用：在文件封盘时，将所有变长区块的绝对坐标打上物理钢印
     * * @param channel       当前正在写入的文件通道
     * @param filterOffset  Bloom Filter Block 的起始偏移量
     * @param indexOffset   Index Block (目录块) 的起始偏移量
     * @param minKeyOffset  最小 Key 序列化块的起始偏移量
     * @param maxKeyOffset  最大 Key 序列化块的起始偏移量
     * @throws IOException  底层磁盘 I/O 异常
     */
    public static void writeFooter(FileChannel channel,
                                   long filterOffset,
                                   long indexOffset,
                                   long minKeyOffset,
                                   long maxKeyOffset) throws IOException {

        // 精确分配 40 字节的堆内存
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);

        // 严格按照约定的物理顺序填入坐标指针
        buffer.putLong(filterOffset);
        buffer.putLong(indexOffset);
        buffer.putLong(minKeyOffset);
        buffer.putLong(maxKeyOffset);
        buffer.putLong(MAGIC_NUMBER);

        // 将 ByteBuffer 从写模式翻转为读模式，准备冲刷到磁盘
        buffer.flip();

        // 事实：受限于操作系统底层缓冲，一次 write 不一定能全写完，必须用 while 循环保底
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}