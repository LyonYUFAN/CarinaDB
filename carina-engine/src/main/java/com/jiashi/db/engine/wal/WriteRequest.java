package  com.jiashi.db.engine.wal;
import java.util.concurrent.CompletableFuture;

public class WriteRequest {
    // 1. 物理事实：当前线程真正想要追加到 WAL 的二进制数据
    public final byte[] data;

    // 2. 并发事实：用于阻塞当前工作线程，并接收 Leader 完成通知的凭证
    public final CompletableFuture<Void> future;

    public WriteRequest(byte[] data) {
        this.data = data;
        // 初始化时，该 Future 处于未完成 (uncompleted) 状态
        this.future = new CompletableFuture<>();
    }
}