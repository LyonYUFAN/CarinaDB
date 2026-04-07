package com.jiashi.db.common.filter;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class BloomFilter {
    private final BitSet bitSet;
    private final int bitSize;
    private final int numHashFunctions;

    // 构造 1：供 Builder 全新创建
    public BloomFilter(int expectedInsertions, double fpp) {
        this.bitSize = (int) (-expectedInsertions * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        this.numHashFunctions = Math.max(1, (int) Math.round((double) bitSize / expectedInsertions * Math.log(2)));
        this.bitSet = new BitSet(bitSize);
    }

    // 构造 2：供 Reader 精准唤醒 (核心修复点！不再需要外部瞎猜参数)
    public BloomFilter(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        // 先剥离出头部的两个参数
        this.bitSize = buffer.getInt();
        this.numHashFunctions = buffer.getInt();
        // 剩下的才是真实的位数组数据
        byte[] bitSetBytes = new byte[buffer.remaining()];
        buffer.get(bitSetBytes);
        this.bitSet = BitSet.valueOf(bitSetBytes);
    }

    public void add(byte[] key) {
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = murmurHash3(key, i);
            // 修复隐患：Math.abs(Integer.MIN_VALUE) 会溢出成负数，改用位运算求正数
            bitSet.set((hash & Integer.MAX_VALUE) % bitSize);
        }
    }

    public boolean mightContain(byte[] key) {
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = murmurHash3(key, i);
            if (!bitSet.get((hash & Integer.MAX_VALUE) % bitSize)) {
                return false;
            }
        }
        return true;
    }

    // 序列化协议升级：[bitSize(4)][numHashFunctions(4)][bitSet_bytes]
    public byte[] serialize() {
        byte[] bitSetBytes = bitSet.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(8 + bitSetBytes.length);
        buffer.putInt(bitSize);
        buffer.putInt(numHashFunctions);
        buffer.put(bitSetBytes);
        return buffer.array();
    }

    private int murmurHash3(byte[] data, int seed) {
        int h = seed;
        for (byte b : data) {
            h = 31 * h + b;
        }
        return h;
    }
}