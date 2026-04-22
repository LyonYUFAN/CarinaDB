package com.jiashi.db.engine.sstable;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.common.model.LogRecordType;

import java.io.*;
import java.nio.file.Path;

/**
 * 流式读取 SSTable(LogRecord)
 */
public class SSTableScanner implements Closeable {

    private final int level;

    private final long fileId;

    private final long dataEndOffset;

    private final DataInputStream inputStream;

    private long currentReadOffset = 0;

    public SSTableScanner(Path path) throws IOException {
        // 获取版本号
        String filename = path.getFileName().toString();
        String nameWithoutExt = filename.substring(0, filename.lastIndexOf('.'));
        String[] parts = nameWithoutExt.split("-");
        this.level = Integer.parseInt(parts[0].substring(1));
        this.fileId = Long.parseLong(parts[1]);

        try(RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")){
            long fileSize = raf.length();
            raf.seek(fileSize - 40);
            this.dataEndOffset = raf.readLong();
        }

        this.inputStream = new DataInputStream(
          new BufferedInputStream(new FileInputStream(path.toFile()),65536)
        );
    }

    /**
     * 判断是否还有数据
     */
    public boolean hasNext() throws IOException {
        return currentReadOffset < dataEndOffset;
    }

    public LogRecord next() throws IOException {
        if(!hasNext())return null;

        byte type = inputStream.readByte();
        int keySize = inputStream.readInt();
        int valSize = inputStream.readInt();
        int ptrSize = inputStream.readInt();
        currentReadOffset += 13;

        byte[] key = new byte[keySize];
        inputStream.readFully(key);
        currentReadOffset += keySize;

        byte[] value = new byte[valSize];
        inputStream.readFully(value);
        currentReadOffset += valSize;

        byte[] pointer = null;
        if (ptrSize > 0) {
            pointer = new byte[ptrSize];
            inputStream.readFully(pointer);
        }
        currentReadOffset += ptrSize;
        return new LogRecord(type, key, value, pointer);
    }

    public long getFileId() {
        return fileId;
    }

    public int getLevel() { return level; }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}

