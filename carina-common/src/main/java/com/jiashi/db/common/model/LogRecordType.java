package com.jiashi.db.common.model;

/**
 * WAL 操作类型定义
 */
public class LogRecordType {
    public static final byte PUT_KV = 0;
    public static final byte PUT_VECTOR = 1;
    public static final byte DELETE = 2;
}