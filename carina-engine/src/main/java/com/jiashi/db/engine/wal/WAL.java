package com.jiashi.db.engine.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead Log 核心类 (支持 Group Commit 组提交机制)
 */
public class WAL {
    private final FileChannel fileChannel;

    // 核心组件 1：无锁并发队列，承载高并发写入请求
    private final ConcurrentLinkedQueue<WriteRequest> queue = new ConcurrentLinkedQueue<>();

    // 核心组件 2：排他锁，用于 Leader 选举和物理写入互斥
    private final ReentrantLock lock = new ReentrantLock();

    // 单次组提交的最大请求数量，防止一次性拼装的 ByteBuffer 过大导致 OOM
    private static final int MAX_BATCH_SIZE = 1000;

    /**
     * 构造函数：初始化 WAL 文件通道
     * @param directory WAL 文件存放目录
     * @param fileName WAL 文件名 (例如 "carina.wal")
     */
    public WAL(String directory, String fileName) throws IOException {
        Path path = Paths.get(directory, fileName);
        // 事实：以 追加 (APPEND)、创建 (CREATE) 和 写入 (WRITE) 模式打开通道
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.fileChannel.position(this.fileChannel.size());
    }

    /**
     * 对外暴露的高并发追加写入方法
     * @param data 要落盘的二进制日志记录
     */
    public void append(ByteBuffer data) {
        WriteRequest request = new WriteRequest(data);

        // 1. 无脑入队：所有线程第一步都是将请求推入并发队列
        queue.offer(request);

        // 2. 身份决断与状态轮询
        while (!request.future.isDone()) {

            if (lock.tryLock()) {
                // ============== Leader 路线 ==============
                try {
                    // 拿到锁后，再确认一次自己的请求是不是已经被上一个 Leader 顺手处理了
                    if (request.future.isDone()) {
                        break;
                    }

                    // 只要队列里还有数据，Leader 就必须一直收割
                    while (!queue.isEmpty()) {
                        drainAndFlushOnce();
                    }
                } finally {
                    lock.unlock(); // 刷盘结束，交出 Leader 权杖
                }
            } else {
                // ============== Follower 路线 ==============
                try {
                    // 成为 Follower，交出 CPU 阻塞等待。
                    // 设定 10 毫秒超时，防止极端并发下的死锁挂起。
                    request.future.get(10, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    // 超时说明可能撞上了 Leader 解锁的极限时间差，继续下一轮 while 循环争抢 Leader
                } catch (Exception e) {
                    throw new RuntimeException("WAL append interrupted or failed", e);
                }
            }
        }
    }

    /**
     * Leader 专属：收割队列、合并数据、强制落盘、集体唤醒
     */
    private void drainAndFlushOnce() {
        List<WriteRequest> batch = new ArrayList<>();
        WriteRequest req;
        int totalBytes = 0;

        // 1. 收割队列，收集当前批次的所有请求
        while ((req = queue.poll()) != null) {
            batch.add(req);
            totalBytes += req.data.limit();
            if (batch.size() >= MAX_BATCH_SIZE) {
                break;
            }
        }
        if (batch.isEmpty()) return;
        try {
            // 2. 数据拼接：将零散的 byte[] 拼装成一个连续的 ByteBuffer
            ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
            for (WriteRequest r : batch) {
                buffer.put(r.data);
            }
            buffer.flip(); // 准备从 buffer 读取数据写入 Channel
            // 3. 物理写入与强制落盘 (将多次 I/O 压缩为 1 次顺序写入)
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            // force(false) 告诉操作系统将文件内容（不强求元数据）强制刷入物理磁盘
            //fileChannel.force(false);
            // 4. 集体唤醒：遍历这批请求，点亮它们的 Future 凭证
            for (WriteRequest r : batch) {
                r.future.complete(null);
            }
        } catch (IOException e) {
            // 发生物理 I/O 灾难，必须通知该批次的所有 Follower 写入失败
            for (WriteRequest r : batch) {
                r.future.completeExceptionally(new RuntimeException("WAL Flush Failed", e));
            }
        }
    }

    /**
     * 优雅关闭 WAL
     */
    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.force(true); // 关闭前做最后一次安全刷盘
            fileChannel.close();
        }
    }

    /**
     * 系统重启时的 WAL 灾难恢复逻辑
     * @param recordConsumer 将解码后的 LogRecord 交给引擎层处理的回调函数
     */
    public void recover(java.util.function.Consumer<com.jiashi.db.common.model.LogRecord> recordConsumer) throws IOException {
        // 事实：将 Channel 的读取指针强制拨回文件头部 (0)
        fileChannel.position(0);
        long fileSize = fileChannel.size();
        long currentPos = 0;

        // Header 永远是固定的 8 字节 (CRC32: 4字节 + PayloadSize: 4字节)
        ByteBuffer headerBuffer = ByteBuffer.allocate(com.jiashi.db.common.coder.LogRecordCoder.HEADER_SIZE);

        System.out.println("WAL Recovery Started. Total WAL Size: " + fileSize + " bytes.");
        int recoverCount = 0;

        while (currentPos < fileSize) {
            headerBuffer.clear();

            // 1. 尝试读取定长 Header
            int readBytes = fileChannel.read(headerBuffer);
            if (readBytes == -1) {
                break; // 正常抵达文件末尾
            }
            if (readBytes < com.jiashi.db.common.coder.LogRecordCoder.HEADER_SIZE) {
                // 致命事实：连 Header 都没写完就断电了，属于 Torn Write，直接终止恢复
                System.err.println("WARN: Incomplete WAL header detected at position " + currentPos + ". Stopping recovery.");
                break;
            }

            headerBuffer.flip();
            int expectedCrc = headerBuffer.getInt();
            int payloadSize = headerBuffer.getInt();

            // 防御性编程：防止读取到损坏的 Header 导致内存超限
            if (payloadSize <= 0 || payloadSize > (1024 * 1024 * 100)) { // 假设单条最大 100MB
                System.err.println("ERROR: Corrupted payload size (" + payloadSize + ") at position " + currentPos + ". Stopping recovery.");
                break;
            }

            // 2. 边界物理校验：磁盘剩余可读字节是否还能满足这个 payloadSize
            if (currentPos + com.jiashi.db.common.coder.LogRecordCoder.HEADER_SIZE + payloadSize > fileSize) {
                System.err.println("WARN: Reached EOF before fully reading payload. Torn Write detected. Stopping recovery.");
                break;
            }

            // 3. 读取 Payload
            ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize);
            readBytes = fileChannel.read(payloadBuffer);
            if (readBytes < payloadSize) {
                System.err.println("WARN: Failed to read full payload. Stopping recovery.");
                break;
            }

            // 4. 数据防伪校验 (CRC32 验证)
            java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
            crc32.update(payloadBuffer.array(), 0, payloadSize);
            int actualCrc = (int) crc32.getValue();

            if (expectedCrc != actualCrc) {
                // 致命事实：数据长度够，但内容已经损坏（比如磁盘坏道或断电写入乱码）
                System.err.println("ERROR: CRC mismatch at position " + currentPos + ". Expected: " + expectedCrc + ", Actual: " + actualCrc + ". Stopping recovery.");
                break;
            }

            // 5. 事实确认无误，开始反序列化，并交给引擎层处理
            payloadBuffer.flip();
            com.jiashi.db.common.model.LogRecord record = com.jiashi.db.common.coder.LogRecordCoder.decodePayload(payloadBuffer);
            recordConsumer.accept(record);

            recoverCount++;
            // 推进游标
            currentPos += com.jiashi.db.common.coder.LogRecordCoder.HEADER_SIZE + payloadSize;
        }

        System.out.println("WAL Recovery Finished. Successfully recovered " + recoverCount + " records.");

        // 事实：如果最后因为 Torn Write 提前退出，我们需要将文件截断 (Truncate) 到有效数据的末尾
        // 防止下次追加写入时，新数据被追加在损坏的残骸后面。
        if (currentPos < fileSize) {
            System.out.println("Truncating corrupted WAL tail. New file size will be: " + currentPos);
            fileChannel.truncate(currentPos);
            fileChannel.force(true);
        }

        // 恢复结束后，将写入指针强制拨回有效数据的末尾，为后续的 append 做好准备
        fileChannel.position(currentPos);
    }
}