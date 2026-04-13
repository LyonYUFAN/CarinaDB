package com.jiashi.db.server;

import com.jiashi.db.api.CarinaKVService;
import com.jiashi.db.engine.CarinaEngine;
import com.jiashi.rpc.common.extension.ExtensionLoader;
import com.jiashi.rpc.core.config.RpcConfig;
import com.jiashi.rpc.core.provider.LocalRegistry;
import com.jiashi.rpc.core.registry.ServiceRegistry;
import com.jiashi.rpc.core.transport.server.NettyServer;
import com.jiashi.rpc.core.transport.server.initializer.RpcServerInitializer;

import java.net.InetSocketAddress;

/**
 * 启动 CarinaDB 服务器
 * 不依赖任何繁重的 Web 容器，纯净的 Java Main 函数启动
 */
public class CarinaServerApplication {

    private static final int PORT = 8080;
    private static final String DB_DIR = "./carina-data";

    public static void main(String[] args) {
        System.out.println(" 🚀 正在启动 CarinaDB Server...");
        try{
            CarinaEngine engine = new CarinaEngine(DB_DIR);

            CarinaKVService kvService = new CarinaKVServiceImpl(engine);

            System.out.println("✅ 底层引擎装载完毕，正在启动 RPC 网络层...");

            RpcConfig config = new RpcConfig();
            config.setServerPort(PORT);
            config.setServerHost("127.0.0.1");
            config.setZkAddress("127.0.0.1:2181");

            String serviceName = CarinaKVService.class.getName();
            LocalRegistry.register(serviceName, kvService);

            ServiceRegistry serviceRegistry = ExtensionLoader.getExtensionLoader(ServiceRegistry.class)
                    .getExtension(config.getRegistryType());
            InetSocketAddress serverAddress = new InetSocketAddress(config.getServerHost(), config.getServerPort());
            serviceRegistry.registerService(serviceName,serverAddress);

            NettyServer server = new NettyServer(config);
            server.start();

            System.out.println("✅ CarinaDB 网络飞升成功！");
            System.out.println("==========================================");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[系统信号] 接收到停机指令，正在安全关闭 CarinaDB...");
                try {
                    engine.close();
                    // 这里也可以加上 rpcServer.stop();
                    System.out.println("✅ CarinaDB 已安全停机。");
                } catch (Exception e) {
                    System.err.println("❌ 停机异常: " + e.getMessage());
                }
            }));

        }catch (Exception ex){
            System.err.println("❌ CarinaDB 启动失败，发生物理级崩溃: " + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }
    }
}

