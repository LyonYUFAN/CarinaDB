package com.jiashi.db.engine.compaction;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.engine.sstable.SSTableScanner;

/**
 * 参与多路归并的实体类
 */
public class MergeEntry {

    private final LogRecord record;

    // 该数据所属的 SSTable 文件编号，数字越大代表数据越新
    private final long fileId;

    // 来源扫描器
    private final SSTableScanner scanner;

    public MergeEntry(LogRecord record, long fileId, SSTableScanner scanner) {
        this.record = record;
        this.fileId = fileId;
        this.scanner = scanner;
    }

    public LogRecord getRecord() { return record; }
    public long getFileId() { return fileId; }
    public SSTableScanner getScanner() { return scanner; }
}