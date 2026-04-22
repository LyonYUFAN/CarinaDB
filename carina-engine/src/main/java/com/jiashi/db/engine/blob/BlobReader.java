package com.jiashi.db.engine.blob;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class BlobReader implements Closeable {

    private final FileChannel channel;
    private final long fileId;

    public BlobReader(Path blobFilePath, long fileId) throws IOException {
        this.fileId = fileId;
        this.channel = FileChannel.open(blobFilePath, StandardOpenOption.READ);
    }

    /**
     * 核心动作: 拿着绝对坐标和维度，精准得到向量
     */
    public float[] readVector(long offset, int vectorDim) throws IOException {
        if(vectorDim <= 0){
            return null;
        }
        int byteSize = vectorDim * 4;
        ByteBuffer buffer = ByteBuffer.allocateDirect(byteSize);

        // 无锁高并发读取
        int bytesRead = channel.read(buffer, offset);

        if (bytesRead != byteSize) {
            throw new IOException("物理寻址失败：期望读取 " + byteSize + " 字节，实际只读到 " + bytesRead + " 字节");
        }

        buffer.flip();
        float[] vector = new float[vectorDim];
        for (int i = 0; i < vectorDim; i++) {
            vector[i] = buffer.getFloat();
        }
        return vector;
    }

    public long getFileId() {
        return fileId;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

