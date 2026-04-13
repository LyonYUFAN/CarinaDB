package com.jiashi.db.server;

import com.jiashi.db.api.CarinaKVService;
import com.jiashi.rpc.core.transport.client.NettyClient;
import com.jiashi.rpc.core.transport.client.proxy.RpcClientProxy;

import java.nio.charset.StandardCharsets;

public class CarinaNetworkTest {

    public static void main(String[] args) {

        try{
            NettyClient nettyClient = new NettyClient();

            RpcClientProxy proxy = new RpcClientProxy(nettyClient);

            CarinaKVService clientProxy = proxy.getProxy(CarinaKVService.class);

            System.out.println("✅ 成功获取 RPC 代理，准备跨进程打击!");

            // 1. 准备极其冷酷的物理数据
            String originKey = "hero:001";
            String originValue = "{\"name\":\"蝙蝠侠\", \"city\":\"哥谭\"}";

            byte[] keyBytes = originKey.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = originValue.getBytes(StandardCharsets.UTF_8);

            // 2. 扣动扳机：通过 OrionRPC 发送 Put 请求
            System.out.println("-> 正在通过网络发送 Put 请求...");
            boolean putResult = clientProxy.put(keyBytes, valueBytes);
            System.out.println("<- Put 操作结果: " + (putResult ? "成功落盘!" : "失败!"));

            // 3. 立刻通过网络读取出来验证
            System.out.println("-> 正在通过网络发送 Get 请求...");
            byte[] resultBytes = clientProxy.get(keyBytes);

            if (resultBytes != null) {
                String resultStr = new String(resultBytes, StandardCharsets.UTF_8);
                System.out.println("<- Get 操作捞回的数据: " + resultStr);
            } else {
                System.out.println("<- Get 操作未能找到数据。");
            }

        } catch (Exception e) {
            System.err.println("❌ 客户端网络请求报错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

