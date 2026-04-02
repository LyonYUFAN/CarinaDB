package com.jiashi.db.engine.sstable;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;

/**
 * SSTable 尾部元数据 (固定 24 字节)
 * 物理结构: MetaIndexOffset(8) + DataIndexOffset(8) + MagicNumber(8)
 */
public class Footer {
    public static final int ENCODED_LENGTH = 24;
    // 独属于 carinaDB 的魔数
    public static final long MAGIC_NUMBER = 0x8A9BCA3D5E6F7890L;

    /**
     * 将 Footer 直接追加到文件末尾
     */
    public static void writeFooter(FileChannel channel, long metaIndexOffset, long dataIndexOffset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(ENCODED_LENGTH);
        buffer.putLong(metaIndexOffset);
        buffer.putLong(dataIndexOffset);
        buffer.putLong(MAGIC_NUMBER);
        buffer.flip(); // 切换为读模式，准备写入 Channel
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}