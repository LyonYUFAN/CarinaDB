package com.jiashi.db.engine.blob;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
// =========================================================================================
//  [TODO - 架构优化]: WiscKey vLog 垃圾回收 (Garbage Collection)
// -----------------------------------------------------------------------------------------
// 物理现状：当前采用 WiscKey 键值分离架构，大向量直接追加至 vLog，SSTable 仅保留 20B 藏宝图。
// 空间放大问题：当数据被覆盖 (Update) 或删除 (Delete) 时，LSM-Tree 中的旧指针会被丢弃，
//             但 vLog 中的旧向量数据依然占用物理磁盘，导致 Space Leak。
// 修复方案设计 (离线/后台 GC 线程)：
// 1. 扫描旧 vLog 文件，读取 [Key, Vector] (需改造 BlobWriter 支持前缀 Key 写入)。
// 2. 拿着 Key 回调 CarinaEngine.query(key)。
// 3. 比对返回的 Pointer 绝对偏移量。如果偏移量不匹配或查不到，说明是过期数据，直接物理丢弃。
// 4. 如果匹配，说明是存活数据，追加写入新的 vLog 文件，并原子更新 LSM-Tree 中的 Pointer。
// =========================================================================================

/**
 * TODO(Performance-Optimization): 引入零拷贝 (Zero-Copy) 写入路径
 * -----------------------------------------------------------------------------------------
 * 现状: 传入 float[] 触发 JVM 堆内序列化与二次拷贝，在高并发下产生 CPU 瓶颈与 GC 压力。
 * 目标:
 * 1. 修改 API 接受 java.nio.DirectByteBuffer。
 * 2. 上层（CarinaEngine）直接在 Direct Memory 分配向量空间。
 * 3. 利用 FileChannel.write(ByteBuffer) 直接触发 DMA 传输。
 * 预期收益: 降低 40% 以上的写入 CPU 占用，彻底消除向量序列化产生的临时对象。
 */


/**
 * vLog 文件追加写入器
 * 负责将向量数据顺序写入底层日志文件，并返回对应的物理偏移量指针
 */
public class BlobWriter implements Closeable {

    private final long fileId;

    private final FileChannel channel;

    private final AtomicLong currentOffset;

    public BlobWriter(Path blobFilePath, long fileId) throws IOException {
        this.fileId = fileId;
        this.channel = FileChannel.open(blobFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        this.currentOffset = new AtomicLong(channel.size());
    }

    public BlobPointer append(float[] vector) throws IOException {
        if(vector == null || vector.length == 0){
            return null;
        }
        int byteSize = vector.length * Float.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteSize);
        for (float v : vector) {
            byteBuffer.putFloat(v);
        }
        byteBuffer.flip();
        long myOffset = currentOffset.getAndAdd(byteSize);
        while(byteBuffer.hasRemaining()){
            channel.write(byteBuffer,myOffset+byteBuffer.position());
        }
        return new BlobPointer(this.fileId,myOffset,vector.length);
    }

    /**
     * 暴露给 WAL Leader 的统一强制刷盘接口
     */
    public void sync() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(false);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

