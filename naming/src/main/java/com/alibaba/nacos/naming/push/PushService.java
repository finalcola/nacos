/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.push;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Subscriber;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * 当服务发送变更时，通过udp协议将最新的服务信息推送给客户端
 * @author nacos
 */
@Component
public class PushService implements ApplicationContextAware, ApplicationListener<ServiceChangeEvent> {

    @Autowired
    private SwitchDomain switchDomain;

    private ApplicationContext applicationContext;

    private static final long ACK_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10L);

    private static final int MAX_RETRY_TIMES = 1;

    private static volatile ConcurrentMap<String, Receiver.AckEntry> ackMap
        = new ConcurrentHashMap<String, Receiver.AckEntry>();

    private static ConcurrentMap<String/*namespaceId##serviceName*/, ConcurrentMap<String/*client.toString*/, PushClient>> clientMap
        = new ConcurrentHashMap<String, ConcurrentMap<String, PushClient>>();

    private static volatile ConcurrentHashMap<String, Long> udpSendTimeMap = new ConcurrentHashMap<String, Long>();

    public static volatile ConcurrentHashMap<String, Long> pushCostMap = new ConcurrentHashMap<String, Long>();

    private static int totalPush = 0;

    private static int failedPush = 0;

    private static ConcurrentHashMap<String, Long> lastPushMillisMap = new ConcurrentHashMap<>();

    private static DatagramSocket udpSocket;

    private static Map<String, Future> futureMap = new ConcurrentHashMap<>();
    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.push.retransmitter");
            return t;
        }
    });

    private static ScheduledExecutorService udpSender = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.push.udpSender");
            return t;
        }
    });

    static {
        try {
            // 创建udpSocket
            udpSocket = new DatagramSocket();

            // 开启Receiver后台线程，用于处理客户端响应
            Receiver receiver = new Receiver();

            Thread inThread = new Thread(receiver);
            inThread.setDaemon(true);
            inThread.setName("com.alibaba.nacos.naming.push.receiver");
            inThread.start();

            // 定期清理僵死的客户端
            executorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        removeClientIfZombie();
                    } catch (Throwable e) {
                        Loggers.PUSH.warn("[NACOS-PUSH] failed to remove client zombie");
                    }
                }
            }, 0, 20, TimeUnit.SECONDS);

        } catch (SocketException e) {
            Loggers.SRV_LOG.error("[NACOS-PUSH] failed to init push service");
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    // service更新事件，将更新推送给client
    @Override
    public void onApplicationEvent(ServiceChangeEvent event) {
        Service service = event.getService();
        String serviceName = service.getName();
        String namespaceId = service.getNamespaceId();
        // 将最新的Service信息推送给注册到该服务上的client
        Future future = udpSender.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    Loggers.PUSH.info(serviceName + " is changed, add it to push queue.");
                    // 获取指定namespace、service的client
                    ConcurrentMap<String, PushClient> clients = clientMap.get(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
                    if (MapUtils.isEmpty(clients)) {
                        return;
                    }

                    // 缓存发送给client的data信息
                    Map<String, Object> cache = new HashMap<>(16);
                    long lastRefTime = System.nanoTime();
                    // 通过udp将最新的数据的发送给service下的每个客户端
                    for (PushClient client : clients.values()) {
                        if (client.zombie()) {
                            Loggers.PUSH.debug("client is zombie: " + client.toString());
                            clients.remove(client.toString());
                            Loggers.PUSH.debug("client is zombie: " + client.toString());
                            continue;
                        }

                        Receiver.AckEntry ackEntry;
                        Loggers.PUSH.debug("push serviceName: {} to client: {}", serviceName, client.toString());
                        String key = getPushCacheKey(serviceName, client.getIp(), client.getAgent());
                        byte[] compressData = null;
                        Map<String, Object> data = null;
                        // 开关中配置的缓存过期时间大于20s，则尝试从缓存中获取
                        if (switchDomain.getDefaultPushCacheMillis() >= 20000 && cache.containsKey(key)) {
                            org.javatuples.Pair pair = (org.javatuples.Pair) cache.get(key);
                            // 缓存数据包括压缩数据和原数据
                            compressData = (byte[]) (pair.getValue0());
                            data = (Map<String, Object>) pair.getValue1();

                            Loggers.PUSH.debug("[PUSH-CACHE] cache hit: {}:{}", serviceName, client.getAddrStr());
                        }

                        // 封装客户端信息和从pushDataSource中获取的数据（会进行压缩）
                        if (compressData != null) {
                            ackEntry = prepareAckEntry(client, compressData, data, lastRefTime);
                        } else {
                            // 读取pushDataSource的数据（Instance列表）,封装为AckEntry
                            ackEntry = prepareAckEntry(client, prepareHostsData(client)/*获取pushDataSource中的data*/, lastRefTime);
                            // 放入缓存
                            if (ackEntry != null) {
                                cache.put(key, new org.javatuples.Pair<>(ackEntry.origin.getData(), ackEntry.data));
                            }
                        }

                        Loggers.PUSH.info("serviceName: {} changed, schedule push for: {}, agent: {}, key: {}",
                            client.getServiceName(), client.getAddrStr(), client.getAgent(), (ackEntry == null ? null : ackEntry.key));

                        // 通过udp发送数据给客户端
                        udpPush(ackEntry);
                    }
                } catch (Exception e) {
                    Loggers.PUSH.error("[NACOS-PUSH] failed to push serviceName: {} to client, error: {}", serviceName, e);

                } finally {
                    futureMap.remove(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
                }

            }
        }, 1000, TimeUnit.MILLISECONDS);
        // 标记当前服务最新信息正在推送给client
        futureMap.put(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName), future);

    }

    public int getTotalPush() {
        return totalPush;
    }

    public void setTotalPush(int totalPush) {
        PushService.totalPush = totalPush;
    }

    public void addClient(String namespaceId,
                          String serviceName,
                          String clusters,
                          String agent,
                          InetSocketAddress socketAddr,
                          DataSource dataSource,
                          String tenant,
                          String app) {

        PushClient client = new PushClient(namespaceId,
            serviceName,
            clusters,
            agent,
            socketAddr,
            dataSource,
            tenant,
            app);
        addClient(client);
    }

    // 添加或刷新client
    public static void addClient(PushClient client) {
        // client is stored by key 'serviceName' because notify event is driven by serviceName change
        String serviceKey = UtilsAndCommons.assembleFullServiceName(client.getNamespaceId(), client.getServiceName());
        ConcurrentMap<String, PushClient> clients =
            clientMap.get(serviceKey);
        if (clients == null) {
            clientMap.putIfAbsent(serviceKey, new ConcurrentHashMap<String, PushClient>(1024));
            clients = clientMap.get(serviceKey);
        }

        // 验证client是否已经存在
        PushClient oldClient = clients.get(client.toString());
        if (oldClient != null) {
            // 更新时间戳信息
            oldClient.refresh();
        } else {
            // 添加client
            PushClient res = clients.putIfAbsent(client.toString(), client);
            if (res != null) {
                Loggers.PUSH.warn("client: {} already associated with key {}", res.getAddrStr(), res.toString());
            }
            Loggers.PUSH.debug("client: {} added for serviceName: {}", client.getAddrStr(), client.getServiceName());
        }
    }

    public List<Subscriber> getClients(String serviceName, String namespaceId) {
        String serviceKey = UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName);
        ConcurrentMap<String, PushClient> clientConcurrentMap = clientMap.get(serviceKey);
        if (Objects.isNull(clientConcurrentMap)) {
            return null;
        }
        List<Subscriber> clients = new ArrayList<Subscriber>();
        clientConcurrentMap.forEach((key, client) -> {
            clients.add(new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(), client.getIp(), namespaceId, serviceName));
        });
        return clients;
    }

    // 移除僵死客户端
    public static void removeClientIfZombie() {

        int size = 0;
        for (Map.Entry<String, ConcurrentMap<String, PushClient>> entry : clientMap.entrySet()) {
            ConcurrentMap<String, PushClient> clientConcurrentMap = entry.getValue();
            for (Map.Entry<String, PushClient> entry1 : clientConcurrentMap.entrySet()) {
                PushClient client = entry1.getValue();
                // 上次响应时间大于限制（默认10s），则将客户端视为僵死
                if (client.zombie()) {
                    clientConcurrentMap.remove(entry1.getKey());
                }
            }

            size += clientConcurrentMap.size();
        }

        Loggers.PUSH.info("[NACOS-PUSH] clientMap size: {}", size);

    }

    // 将data和客户端地址信息封装为AckEntry
    private static Receiver.AckEntry prepareAckEntry(PushClient client, byte[] dataBytes, Map<String, Object> data,
                                                     long lastRefTime) {
        String key = getACKKey(client.getSocketAddr().getAddress().getHostAddress(),
            client.getSocketAddr().getPort(),
            lastRefTime);
        DatagramPacket packet = null;
        try {
            packet = new DatagramPacket(dataBytes, dataBytes.length, client.socketAddr);
            Receiver.AckEntry ackEntry = new Receiver.AckEntry(key, packet);
            ackEntry.data = data;

            // we must store the key be fore send, otherwise there will be a chance the
            // ack returns before we put in
            ackEntry.data = data;

            return ackEntry;
        } catch (Exception e) {
            Loggers.PUSH.error("[NACOS-PUSH] failed to prepare data: {} to client: {}, error: {}",
                data, client.getSocketAddr(), e);
        }

        return null;
    }

    public static String getPushCacheKey(String serviceName, String clientIP, String agent) {
        return serviceName + UtilsAndCommons.CACHE_KEY_SPLITER + agent;
    }

    // 通知服务变更事件
    public void serviceChanged(Service service) {
        // merge some change events to reduce the push frequency:
        // 合并更新事件，减少push次数
        if (futureMap.containsKey(UtilsAndCommons.assembleFullServiceName(service.getNamespaceId(), service.getName()))) {
            return;
        }

        // 通知服务变更事件(PushService本身也监听了该事件)
        this.applicationContext.publishEvent(new ServiceChangeEvent(this, service));
    }

    // 是否可开启push服务
    public boolean canEnablePush(String agent) {

        // 检查开关
        if (!switchDomain.isPushEnabled()) {
            return false;
        }

        // 解析客户端语言类型和版本
        ClientInfo clientInfo = new ClientInfo(agent);

        // 检查语言的版本
        if (ClientInfo.ClientType.JAVA == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushJavaVersion())) >= 0) {
            return true;
        } else if (ClientInfo.ClientType.DNS == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushPythonVersion())) >= 0) {
            return true;
        } else if (ClientInfo.ClientType.C == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushCVersion())) >= 0) {
            return true;
        } else if (ClientInfo.ClientType.GO == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushGoVersion())) >= 0) {
            return true;
        }

        return false;
    }

    public static List<Receiver.AckEntry> getFailedPushes() {
        return new ArrayList<Receiver.AckEntry>(ackMap.values());
    }

    public int getFailedPushCount() {
        return ackMap.size() + failedPush;
    }

    public void setFailedPush(int failedPush) {
        PushService.failedPush = failedPush;
    }


    public static void resetPushState() {
        ackMap.clear();
    }

    public class PushClient {
        private String namespaceId;
        private String serviceName;
        private String clusters;
        private String agent;
        private String tenant;
        private String app;
        private InetSocketAddress socketAddr;
        private DataSource dataSource;
        private Map<String, String[]> params;

        public Map<String, String[]> getParams() {
            return params;
        }

        public void setParams(Map<String, String[]> params) {
            this.params = params;
        }

        public long lastRefTime = System.currentTimeMillis();

        public PushClient(String namespaceId,
                          String serviceName,
                          String clusters,
                          String agent,
                          InetSocketAddress socketAddr,
                          DataSource dataSource,
                          String tenant,
                          String app) {
            this.namespaceId = namespaceId;
            this.serviceName = serviceName;
            this.clusters = clusters;
            this.agent = agent;
            this.socketAddr = socketAddr;
            this.dataSource = dataSource;
            this.tenant = tenant;
            this.app = app;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public PushClient(InetSocketAddress socketAddr) {
            this.socketAddr = socketAddr;
        }

        // 上次响应时间大于限制（默认10s），则将客户端视为僵死
        public boolean zombie() {
            return System.currentTimeMillis() - lastRefTime > switchDomain.getPushCacheMillis(serviceName);
        }

        @Override
        public String toString() {
            return "serviceName: " + serviceName
                + ", clusters: " + clusters
                + ", ip: " + socketAddr.getAddress().getHostAddress()
                + ", port: " + socketAddr.getPort()
                + ", agent: " + agent;
        }

        public String getAgent() {
            return agent;
        }

        public String getAddrStr() {
            return socketAddr.getAddress().getHostAddress() + ":" + socketAddr.getPort();
        }

        public String getIp() {
            return socketAddr.getAddress().getHostAddress();
        }

        @Override
        public int hashCode() {
            return Objects.hash(serviceName, clusters, socketAddr);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PushClient)) {
                return false;
            }

            PushClient other = (PushClient) obj;

            return serviceName.equals(other.serviceName) && clusters.equals(other.clusters) && socketAddr.equals(other.socketAddr);
        }

        public String getClusters() {
            return clusters;
        }

        public void setClusters(String clusters) {
            this.clusters = clusters;
        }

        public String getNamespaceId() {
            return namespaceId;
        }

        public void setNamespaceId(String namespaceId) {
            this.namespaceId = namespaceId;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getTenant() {
            return tenant;
        }

        public void setTenant(String tenant) {
            this.tenant = tenant;
        }

        public String getApp() {
            return app;
        }

        public void setApp(String app) {
            this.app = app;
        }

        public InetSocketAddress getSocketAddr() {
            return socketAddr;
        }

        public void refresh() {
            lastRefTime = System.currentTimeMillis();
        }
    }

    private static byte[] compressIfNecessary(byte[] dataBytes) throws IOException {
        // enable compression when data is larger than 1KB
        int maxDataSizeUncompress = 1024;
        if (dataBytes.length < maxDataSizeUncompress) {
            return dataBytes;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(dataBytes);
        gzip.close();

        return out.toByteArray();
    }

    private static Map<String, Object> prepareHostsData(PushClient client) throws Exception {
        Map<String, Object> cmd = new HashMap<String, Object>(2);
        cmd.put("type", "dom");
        cmd.put("data", client.getDataSource().getData(client));

        return cmd;
    }

    private static Receiver.AckEntry prepareAckEntry(PushClient client, Map<String, Object> data, long lastRefTime) {
        if (MapUtils.isEmpty(data)) {
            Loggers.PUSH.error("[NACOS-PUSH] pushing empty data for client is not allowed: {}", client);
            return null;
        }

        data.put("lastRefTime", lastRefTime);

        // we apply lastRefTime as sequence num for further ack
        String key = getACKKey(client.getSocketAddr().getAddress().getHostAddress(),
            client.getSocketAddr().getPort(),
            lastRefTime);

        String dataStr = JSON.toJSONString(data);

        try {
            byte[] dataBytes = dataStr.getBytes(StandardCharsets.UTF_8);
            // 压缩数据
            dataBytes = compressIfNecessary(dataBytes);

            DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length, client.socketAddr);

            // we must store the key be fore send, otherwise there will be a chance the
            // ack returns before we put in
            Receiver.AckEntry ackEntry = new Receiver.AckEntry(key, packet);
            ackEntry.data = data;

            return ackEntry;
        } catch (Exception e) {
            Loggers.PUSH.error("[NACOS-PUSH] failed to prepare data: {} to client: {}, error: {}",
                data, client.getSocketAddr(), e);
            return null;
        }
    }

    // 将集群最新的集群信息推送给client
    private static Receiver.AckEntry udpPush(Receiver.AckEntry ackEntry) {
        if (ackEntry == null) {
            Loggers.PUSH.error("[NACOS-PUSH] ackEntry is null.");
            return null;
        }

        // 默认可重试一次
        if (ackEntry.getRetryTimes() > MAX_RETRY_TIMES) {
            Loggers.PUSH.warn("max re-push times reached, retry times {}, key: {}", ackEntry.retryTimes, ackEntry.key);
            // 删除任务
            ackMap.remove(ackEntry.key);
            udpSendTimeMap.remove(ackEntry.key);
            failedPush += 1;
            return ackEntry;
        }

        try {
            // push次数+1
            if (!ackMap.containsKey(ackEntry.key)) {
                totalPush++;
            }
            // 记录该任务
            ackMap.put(ackEntry.key, ackEntry);
            udpSendTimeMap.put(ackEntry.key, System.currentTimeMillis());

            Loggers.PUSH.info("send udp packet: " + ackEntry.key);
            // 推送数据给client
            udpSocket.send(ackEntry.origin);

            ackEntry.increaseRetryTime();


            // 添加超时后重新发送
            executorService.schedule(new Retransmitter(ackEntry), TimeUnit.NANOSECONDS.toMillis(ACK_TIMEOUT_NANOS),
                TimeUnit.MILLISECONDS);

            return ackEntry;
        } catch (Exception e) {
            Loggers.PUSH.error("[NACOS-PUSH] failed to push data: {} to client: {}, error: {}",
                ackEntry.data, ackEntry.origin.getAddress().getHostAddress(), e);
            ackMap.remove(ackEntry.key);
            udpSendTimeMap.remove(ackEntry.key);
            failedPush += 1;

            return null;
        }
    }

    private static String getACKKey(String host, int port, long lastRefTime) {
        return StringUtils.strip(host) + "," + port + "," + lastRefTime;
    }

    // 用于超时后重新发送
    public static class Retransmitter implements Runnable {
        Receiver.AckEntry ackEntry;

        public Retransmitter(Receiver.AckEntry ackEntry) {
            this.ackEntry = ackEntry;
        }

        @Override
        public void run() {
            // 如果发送失败，则ackEntry依然保存在ackMap中，重试
            if (ackMap.containsKey(ackEntry.key)) {
                Loggers.PUSH.info("retry to push data, key: " + ackEntry.key);
                udpPush(ackEntry);
            }
        }
    }

    public static class Receiver implements Runnable {
        @Override
        public void run() {
            while (true) {
                byte[] buffer = new byte[1024 * 64];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                try {
                    // 接收数据
                    udpSocket.receive(packet);

                    // 解码
                    String json = new String(packet.getData(), 0, packet.getLength(), Charset.forName("UTF-8")).trim();
                    AckPacket ackPacket = JSON.parseObject(json, AckPacket.class);

                    InetSocketAddress socketAddress = (InetSocketAddress) packet.getSocketAddress();
                    String ip = socketAddress.getAddress().getHostAddress();
                    int port = socketAddress.getPort();

                    // 超时
                    if (System.nanoTime() - ackPacket.lastRefTime > ACK_TIMEOUT_NANOS) {
                        Loggers.PUSH.warn("ack takes too long from {} ack json: {}", packet.getSocketAddress(), json);
                    }

                    String ackKey = getACKKey(ip, port, ackPacket.lastRefTime);
                    AckEntry ackEntry = ackMap.remove(ackKey);
                    if (ackEntry == null) {
                        throw new IllegalStateException("unable to find ackEntry for key: " + ackKey
                            + ", ack json: " + json);
                    }

                    // 花费时间
                    long pushCost = System.currentTimeMillis() - udpSendTimeMap.get(ackKey);

                    Loggers.PUSH.info("received ack: {} from: {}:, cost: {} ms, unacked: {}, total push: {}",
                        json, ip, port, pushCost, ackMap.size(), totalPush);

                    // 记录时间
                    pushCostMap.put(ackKey, pushCost);
                    // 标记当前service已经推送完成
                    udpSendTimeMap.remove(ackKey);

                } catch (Throwable e) {
                    Loggers.PUSH.error("[NACOS-PUSH] error while receiving ack data", e);
                }
            }
        }

        public static class AckEntry {

            public AckEntry(String key, DatagramPacket packet) {
                this.key = key;
                this.origin = packet;
            }

            public void increaseRetryTime() {
                retryTimes.incrementAndGet();
            }

            public int getRetryTimes() {
                return retryTimes.get();
            }

            public String key;
            // udp数据包
            public DatagramPacket origin;
            private AtomicInteger retryTimes = new AtomicInteger(0);
            // 原数据
            public Map<String, Object> data;
        }

        public static class AckPacket {
            public String type;
            public long lastRefTime;

            public String data;
        }
    }


}
