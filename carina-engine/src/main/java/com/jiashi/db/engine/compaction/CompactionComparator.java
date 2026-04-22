package com.jiashi.db.engine.compaction;

import java.util.Comparator;

import static java.lang.Math.min;

/**
 * 多路归并排序规则:
 * Key 小的优先出列
 * Key 相同时, FileId 大的优先出列
 */
public class CompactionComparator implements Comparator<MergeEntry> {


    @Override
    public int compare(MergeEntry e1, MergeEntry e2) {
        byte[] key1 = e1.getRecord().getKey();
        byte[] key2 = e2.getRecord().getKey();

        int keyCmp = compare(key1, key2);
        if (keyCmp != 0) {
            return keyCmp;
        }

        return Long.compare(e2.getFileId(), e1.getFileId());
    }

    /**
     * 字节数组字典序比较器
     * @param key1
     * @param key2
     * @return
     */
    private int compare(byte[] key1, byte[] key2) {
        int minLen = min(key1.length, key2.length);
        for (int i = 0; i < minLen; i++) {
            int u1 = key1[i] & 0xFF;;
            int u2 = key2[i] & 0xFF;
            if(u1 != u2){
                return u1 - u2;
            }
        }
        return key1.length - key2.length;
    }
}

