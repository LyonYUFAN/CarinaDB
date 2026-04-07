package com.jiashi.db.engine.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 块缓存：控制内存上限，防止 OOM
 */
public class BlockCache<K, V> {
    private final int maxCapacity;
    private final Map<K, V> cache;

    public BlockCache(int maxCapacity) {
        this.maxCapacity = maxCapacity;
        // 利用 LinkedHashMap 的 accessOrder 属性天然实现 LRU
        this.cache = new LinkedHashMap<K, V>(maxCapacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > BlockCache.this.maxCapacity; // 超过容量就踢掉最老的
            }
        };
    }

    public synchronized void put(K key, V value) {
        cache.put(key, value);
    }

    public synchronized V get(K key) {
        return cache.get(key);
    }
}