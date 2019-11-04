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
package com.alibaba.nacos.naming.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.core.utils.SystemUtils.*;

/**
 * The manager to globally refresh and operate server list.
 *  1、更新server列表
 *  2、更新节点心跳任务，并向其它server发送当前server状态
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component("serverListManager")
public class ServerListManager {

    private static final int STABLE_PERIOD = 60 * 1000;

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 监听server列表变更
     */
    private List<ServerChangeListener> listeners = new ArrayList<>();

    /**
     * server列表(保存地址、时间戳信息)
     */
    private List<Server> servers = new ArrayList<>();

    /**
     * 健康节点
     */
    private List<Server> healthyServers = new ArrayList<>();

    private Map<String/*site*/, List<Server>> distroConfig = new ConcurrentHashMap<>();

    private Set<String> liveSites = new HashSet<>();

    private final static String LOCALHOST_SITE = UtilsAndCommons.UNKNOWN_SITE;

    private long lastHealthServerMillis = 0L;

    private boolean autoDisabledHealthCheck = false;

    /**
     * 向其他server发送本地server状态
     */
    private Synchronizer synchronizer = new ServerStatusSynchronizer();

    public void listen(ServerChangeListener listener) {
        listeners.add(listener);
    }

    @PostConstruct
    public void init() {
        // 更新server列表任务(配置文件或systemEnv读取)
        GlobalExecutor.registerServerListUpdater(new ServerListUpdater());
        // 更新本地server心跳，向其它存活server发送本地server状态信息
        GlobalExecutor.registerServerStatusReporter(new ServerStatusReporter(), 5000);
    }

    /**
     * 刷新server列表(非单机模式),配置文件 < systemEnv
     * @return
     */
    private List<Server> refreshServerList() {

        List<Server> result = new ArrayList<>();

        // 单机模式，返回本地接节点
        if (STANDALONE_MODE) {
            Server server = new Server();
            server.setIp(NetUtils.getLocalAddress());
            server.setServePort(RunningConfig.getServerPort());
            result.add(server);
            return result;
        }

        List<String> serverList = new ArrayList<>();
        try {
            // 读取配置文件里的server地址
            serverList = readClusterConf();
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("failed to get config: " + CLUSTER_CONF_FILE_PATH, e);
        }

        if (Loggers.DEBUG_LOG.isDebugEnabled()) {
            Loggers.DEBUG_LOG.debug("SERVER-LIST from cluster.conf: {}", result);
        }

        //use system env
        // 如果配置文件为空，读取systemDev
        if (CollectionUtils.isEmpty(serverList)) {
            serverList = SystemUtils.getIPsBySystemEnv(UtilsAndCommons.SELF_SERVICE_CLUSTER_ENV);
            if (Loggers.DEBUG_LOG.isDebugEnabled()) {
                Loggers.DEBUG_LOG.debug("SERVER-LIST from system variable: {}", result);
            }
        }

        // 封装ip、port，返回server列表
        if (CollectionUtils.isNotEmpty(serverList)) {

            for (int i = 0; i < serverList.size(); i++) {

                String ip;
                int port;
                String server = serverList.get(i);
                if (server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
                    ip = server.split(UtilsAndCommons.IP_PORT_SPLITER)[0];
                    port = Integer.parseInt(server.split(UtilsAndCommons.IP_PORT_SPLITER)[1]);
                } else {
                    ip = server;
                    port = RunningConfig.getServerPort();
                }

                Server member = new Server();
                member.setIp(ip);
                member.setServePort(port);
                result.add(member);
            }
        }

        return result;
    }

    /**
     * 地址是否在server列表中
     */
    public boolean contains(String s) {
        for (Server server : servers) {
            if (server.getKey().equals(s)) {
                return true;
            }
        }
        return false;
    }

    public List<Server> getServers() {
        return servers;
    }

    public List<Server> getHealthyServers() {
        return healthyServers;
    }

    private void notifyListeners() {

        GlobalExecutor.notifyServerListChange(new Runnable() {
            @Override
            public void run() {
                for (ServerChangeListener listener : listeners) {
                    listener.onChangeServerList(servers);
                    listener.onChangeHealthyServerList(healthyServers);
                }
            }
        });
    }

    public Map<String, List<Server>> getDistroConfig() {
        return distroConfig;
    }

    // 接收到其他节点或本地节点的心跳（参数包含其状态：site:ip:lastReportTime:weight）
    public synchronized void onReceiveServerStatus(String configInfo) {

        Loggers.SRV_LOG.info("receive config info: {}", configInfo);

        String[] configs = configInfo.split("\r\n");
        if (configs.length == 0) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>();
        List<Server> tmpServerList = new ArrayList<>();

        for (String config : configs) {
            tmpServerList.clear();
            // site:ip:lastReportTime:weight
            String[] params = config.split("#");
            if (params.length <= 3) {
                Loggers.SRV_LOG.warn("received malformed distro map data: {}", config);
                continue;
            }

            Server server = new Server();

            server.setSite(params[0]);
            server.setIp(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[0]);
            server.setServePort(Integer.parseInt(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[1]));
            server.setLastRefTime(Long.parseLong(params[2]));

            // 是否在server列表中
            if (!contains(server.getKey())) {
                throw new IllegalArgumentException("server: " + server.getKey() + " is not in serverlist");
            }

            // 上次心跳时间
            Date date = new Date(Long.parseLong(params[2]));
            server.setLastRefTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));

            // 权重
            server.setWeight(params.length == 4 ? Integer.parseInt(params[3]) : 1);
            // 心跳间隔是否小于过期时间
            server.setAlive(System.currentTimeMillis() - server.getLastRefTime() < switchDomain.getDistroServerExpiredMillis());
            // 添加到distroConfig
            List<Server> list = distroConfig.get(server.getSite());
            if (list == null || list.size() <= 0) {
                list = new ArrayList<>();
                list.add(server);
                distroConfig.put(server.getSite(), list);
            }

            // 更新distroConfig中site server列表中的server状态
            for (Server s : list) {
                String serverId = s.getKey() + "_" + s.getSite();
                String newServerId = server.getKey() + "_" + server.getSite();

                if (serverId.equals(newServerId)) {
                    if (s.isAlive() != server.isAlive() || s.getWeight() != server.getWeight()) {
                        Loggers.SRV_LOG.warn("server beat out of date, current: {}, last: {}",
                            JSON.toJSONString(server), JSON.toJSONString(s));
                    }
                    tmpServerList.add(server);
                    continue;
                }
                tmpServerList.add(s);
            }

            if (!tmpServerList.contains(server)) {
                tmpServerList.add(server);
            }

            distroConfig.put(server.getSite(), tmpServerList);
        }
        // 更新存活的site
        liveSites.addAll(distroConfig.keySet());

        List<Server> servers = distroConfig.get(LOCALHOST_SITE);
        if (CollectionUtils.isEmpty(servers)) {
            return;
        }

        //local site servers
        // 更新healthyServers(重复次数代表权重)
        List<String> allLocalSiteSrvs = new ArrayList<>();
        for (Server server : servers) {

            if (server.getKey().endsWith(":0")) {
                continue;
            }

            // 地址的权重
            server.setAdWeight(switchDomain.getAdWeight(server.getKey()) == null ? 0 : switchDomain.getAdWeight(server.getKey()));

            // 权重代表list中的重复次数
            for (int i = 0; i < server.getWeight() + server.getAdWeight(); i++) {

                if (!allLocalSiteSrvs.contains(server.getKey())) {
                    allLocalSiteSrvs.add(server.getKey());
                }

                if (server.isAlive() && !newHealthyList.contains(server)) {
                    newHealthyList.add(server);
                }
            }
        }

        Collections.sort(newHealthyList);
        // 存活率
        float curRatio = (float) newHealthyList.size() / allLocalSiteSrvs.size();

        // 存活率达到临界值
        if (autoDisabledHealthCheck
            && curRatio > switchDomain.getDistroThreshold()
            && System.currentTimeMillis() - lastHealthServerMillis > STABLE_PERIOD) {
            Loggers.SRV_LOG.info("[NACOS-DISTRO] distro threshold restored and " +
                "stable now, enable health check. current ratio: {}", curRatio);

            switchDomain.setHealthCheckEnabled(true);

            // we must set this variable, otherwise it will conflict with user's action
            autoDisabledHealthCheck = false;
        }

        if (!CollectionUtils.isEqualCollection(healthyServers, newHealthyList)) {
            // 存活server列表发生了更新,暂时关闭健康检查
            // for every change disable healthy check for some while
            if (switchDomain.isHealthCheckEnabled()) {
                Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, " +
                        "disable health check for {} ms from now on, old: {}, new: {}", STABLE_PERIOD,
                    healthyServers, newHealthyList);

                switchDomain.setHealthCheckEnabled(false);
                autoDisabledHealthCheck = true;

                lastHealthServerMillis = System.currentTimeMillis();
            }

            // 更新健康节列表，并通知listener
            healthyServers = newHealthyList;
            notifyListeners();
        }
    }

    public void clean() {
        cleanInvalidServers();

        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            for (Server server : entry.getValue()) {
                //request other server to clean invalid servers
                if (!server.getKey().equals(NetUtils.localServer())) {
                    requestOtherServerCleanInvalidServers(server.getKey());
                }
            }

        }
    }

    public Set<String> getLiveSites() {
        return liveSites;
    }

    private void cleanInvalidServers() {

        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            List<Server> tmpServers = null;
            List<Server> currentServerList = entry.getValue();

            for (Server server : entry.getValue()) {
                if (!server.isAlive()) {

                    tmpServers = new ArrayList<>();

                    for (Server server1 : currentServerList) {
                        String serverKey1 = server1.getKey() + "_" + server1.getSite();
                        String serverKey = server.getKey() + "_" + server.getSite();

                        if (!serverKey.equals(serverKey1) && !tmpServers.contains(server1)) {
                            tmpServers.add(server1);
                        }
                    }
                }
            }
            if (tmpServers != null) {
                distroConfig.put(entry.getKey(), tmpServers);
            }
        }
    }

    private void requestOtherServerCleanInvalidServers(String serverIP) {
        Map<String, String> params = new HashMap<String, String>(1);

        params.put("action", "without-diamond-clean");
        try {
            NamingProxy.reqAPI("distroStatus", params, serverIP, false);
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("[DISTRO-STATUS-CLEAN] Failed to request to clean server status to " + serverIP, e);
        }
    }

    /**
     * 更新server列表任务
     */
    public class ServerListUpdater implements Runnable {

        @Override
        public void run() {
            try {
                // 刷新server列表(非单机模式),配置文件 < systemEnv
                List<Server> refreshedServers = refreshServerList();
                List<Server> oldServers = servers;

                if (CollectionUtils.isEmpty(refreshedServers)) {
                    Loggers.RAFT.warn("refresh server list failed, ignore it.");
                    return;
                }

                boolean changed = false;

                // 新添加server
                List<Server> newServers = (List<Server>) CollectionUtils.subtract(refreshedServers, oldServers);
                if (CollectionUtils.isNotEmpty(newServers)) {
                    servers.addAll(newServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, new: {} servers: {}", newServers.size(), newServers);
                }

                // 已删除server
                List<Server> deadServers = (List<Server>) CollectionUtils.subtract(oldServers, refreshedServers);
                if (CollectionUtils.isNotEmpty(deadServers)) {
                    servers.removeAll(deadServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, dead: {}, servers: {}", deadServers.size(), deadServers);
                }

                // 通知listener，server列表发生了更新
                if (changed) {
                    notifyListeners();
                }

            } catch (Exception e) {
                Loggers.RAFT.info("error while updating server list.", e);
            }
        }
    }


    /**
     * 更新本地server心跳，向其它存活server发送本地server状态信息
     */
    private class ServerStatusReporter implements Runnable {

        @Override
        public void run() {
            try {

                if (RunningConfig.getServerPort() <= 0) {
                    return;
                }

                // 检查server是否心跳过期
                for (String key : distroConfig.keySet()) {
                    for (Server server : distroConfig.get(key)) {
                        server.setAlive(System.currentTimeMillis() - server.getLastRefTime() < switchDomain.getDistroServerExpiredMillis());
                    }
                }

                // CPU个数/2 作为权重
                int weight = Runtime.getRuntime().availableProcessors() / 2;
                if (weight <= 0) {
                    weight = 1;
                }

                long curTime = System.currentTimeMillis();
                // 本地节点状态
                String status = LOCALHOST_SITE + "#" + NetUtils.localServer() + "#" + curTime + "#" + weight + "\r\n";

                //send status to itself
                // 更新本地server的状态
                onReceiveServerStatus(status);

                List<Server> allServers = getServers();

                if (!contains(NetUtils.localServer())) {
                    Loggers.SRV_LOG.error("local ip is not in serverlist, ip: {}, serverlist: {}", NetUtils.localServer(), allServers);
                    return;
                }

                // 向其它节点发送本地server的状态
                if (allServers.size() > 0 && !NetUtils.localServer().contains(UtilsAndCommons.LOCAL_HOST_IP)) {
                    for (com.alibaba.nacos.naming.cluster.servers.Server server : allServers) {
                        // 跳过本地节点
                        if (server.getKey().equals(NetUtils.localServer())) {
                            continue;
                        }

                        Message msg = new Message();
                        msg.setData(status);

                        // 同步给其它节点
                        synchronizer.send(server.getKey(), msg);

                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[SERVER-STATUS] Exception while sending server status", e);
            } finally {
                GlobalExecutor.registerServerStatusReporter(this, switchDomain.getServerStatusSynchronizationPeriodMillis());
            }

        }
    }
}
