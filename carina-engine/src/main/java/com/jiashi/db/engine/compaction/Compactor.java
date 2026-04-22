package com.jiashi.db.engine.compaction;

import com.jiashi.db.common.model.LogRecord;
import com.jiashi.db.common.model.LogRecordType;
import com.jiashi.db.engine.sstable.SSTableBuilder;
import com.jiashi.db.engine.sstable.SSTableScanner;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

public class Compactor {

    private final String dbPath;

    public Compactor(String dbPath) {
        this.dbPath = dbPath;
    }

    public Path executeCompaction(List<SSTableScanner> scanners, int targetLevel, long newFileId, boolean isBottomLevel) throws IOException {
        PriorityQueue<MergeEntry> heap = new PriorityQueue<>(new CompactionComparator());
        for (SSTableScanner scanner : scanners) {
            if(scanner.hasNext()){
                heap.offer(new MergeEntry(scanner.next(), scanner.getFileId(), scanner));
            }
        }
        Path newFilePath = Paths.get(dbPath, "L" + targetLevel + "-" + newFileId + ".sst");
        SSTableBuilder builder = new SSTableBuilder(newFilePath, 10000);

        byte[] lastProcessedKey = null;
        // 多路归并核心代码
        while(!heap.isEmpty()){
            MergeEntry currentEntry = heap.poll();
            LogRecord record = currentEntry.getRecord();
            byte[] currentKey = record.getKey();
            if (lastProcessedKey != null && Arrays.equals(lastProcessedKey, currentKey)) {
                advanceScanner(heap, currentEntry);
                continue;
            }
            lastProcessedKey = currentKey;
            if (record.getType() == LogRecordType.DELETE && isBottomLevel) {
                advanceScanner(heap, currentEntry);
                continue;
            }
            builder.add(record.getKey(),record.getValue(),record.getType(),record.getBlobPointer());

            advanceScanner(heap,currentEntry);
        }
        builder.finish();
        return newFilePath;
    }

    private void advanceScanner(PriorityQueue<MergeEntry> heap, MergeEntry currentEntry) throws IOException {
        SSTableScanner scanner = currentEntry.getScanner();
        if(scanner.hasNext()){
            heap.offer(new MergeEntry(scanner.next(), currentEntry.getFileId(), scanner));
        }
    }
}

