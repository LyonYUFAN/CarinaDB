package com.jiashi.db.api;

import java.util.List;

public interface CarinaKVService {

    boolean put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    boolean delete(byte[] key);

    /**
     * 核心扩展：写入带有向量的数据
     * @param key    主键
     * @param value  附加元数据 (比如图片的 URL、文本内容)
     * @param vector 高维向量 (比如大模型生成的 Embedding)
     */
    boolean putVector(byte[] key, byte[] value, float[] vector);

    // 向量相似度检索 (Top-K)
    List<byte[]> searchVector(float[] vector, int topK);
}