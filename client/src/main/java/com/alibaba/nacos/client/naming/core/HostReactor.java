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
package com.alibaba.nacos.client.naming.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.utils.StringUtils;

import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author xuanyin
 */
public class HostReactor {

    private static final long DEFAULT_DELAY = 1000L;

    private static final long UPDATE_HOLD_INTERVAL = 5000L;

    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();

    private Map<String, ServiceInfo> serviceInfoMap;

    /**
     * 标记serviceInfo正在更新
     */
    private Map<String, Object> updatingMap;

    /**
     * 接收server push信息的组件，并会将变更通知给相关listener
     */
    private PushReceiver pushReceiver;

    /**
     * 事件通知组件
     */
    private EventDispatcher eventDispatcher;

    /**
     * API代理类
     */
    private NamingProxy serverProxy;

    /**
     * 故障转移开关、缓存相关组件
     */
    private FailoverReactor failoverReactor;

    private String cacheDir;

    private ScheduledExecutorService executor;

    public HostReactor(EventDispatcher eventDispatcher, NamingProxy serverProxy, String cacheDir) {
        this(eventDispatcher, serverProxy, cacheDir, false, UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    public HostReactor(EventDispatcher eventDispatcher, NamingProxy serverProxy, String cacheDir,
                       boolean loadCacheAtStart, int pollingThreadCount) {

        executor = new ScheduledThreadPoolExecutor(pollingThreadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.client.naming.updater");
                return thread;
            }
        });

        this.eventDispatcher = eventDispatcher;
        this.serverProxy = serverProxy;
        this.cacheDir = cacheDir;
        if (loadCacheAtStart) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }

        this.updatingMap = new ConcurrentHashMap<String, Object>();
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        this.pushReceiver = new PushReceiver(this);
    }

    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    public synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    // 处理接收的service信息
    public ServiceInfo processServiceJSON(String json) {
        ServiceInfo serviceInfo = JSON.parseObject(json, ServiceInfo.class);
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());
        // 返回的数据错误
        if (serviceInfo.getHosts() == null || !serviceInfo.validate()) {
            //empty or error push, just ignore
            return oldService;
        }

        if (oldService != null) {
            // 更新serviceInfo

            if (oldService.getLastRefTime() > serviceInfo.getLastRefTime()) {
                NAMING_LOGGER.warn("out of date data received, old-t: " + oldService.getLastRefTime()
                    + ", new-t: " + serviceInfo.getLastRefTime());
            }

            // 更新serviceInfo
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);

            // 原host列表
            Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
            for (Instance host : oldService.getHosts()) {
                oldHostMap.put(host.toInetAddr(), host);
            }

            // 新host列表
            Map<String, Instance> newHostMap = new HashMap<String, Instance>(serviceInfo.getHosts().size());
            for (Instance host : serviceInfo.getHosts()) {
                newHostMap.put(host.toInetAddr(), host);
            }

            Set<Instance> modHosts = new HashSet<Instance>();
            Set<Instance> newHosts = new HashSet<Instance>();
            Set<Instance> remvHosts = new HashSet<Instance>();

            List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                newHostMap.entrySet());
            for (Map.Entry<String, Instance> entry : newServiceHosts) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(),
                    oldHostMap.get(key).toString())) {
                    modHosts.add(host);
                    continue;
                }

                if (!oldHostMap.containsKey(key)) {
                    newHosts.add(host);
                }
            }

            for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (newHostMap.containsKey(key)) {
                    continue;
                }

                if (!newHostMap.containsKey(key)) {
                    remvHosts.add(host);
                }

            }

            // 打印新增、删除、更改的hosts
            if (newHosts.size() > 0) {
                NAMING_LOGGER.info("new ips(" + newHosts.size() + ") service: "
                    + serviceInfo.getName() + " -> " + JSON.toJSONString(newHosts));
            }

            if (remvHosts.size() > 0) {
                NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: "
                    + serviceInfo.getName() + " -> " + JSON.toJSONString(remvHosts));
            }

            if (modHosts.size() > 0) {
                NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: "
                    + serviceInfo.getName() + " -> " + JSON.toJSONString(modHosts));
            }

            serviceInfo.setJsonFromServer(json);

            // 如果列表发生了修改，发送service更新通知，并写入磁盘
            if (newHosts.size() > 0 || remvHosts.size() > 0 || modHosts.size() > 0) {
                eventDispatcher.serviceChanged(serviceInfo);
                DiskCache.write(serviceInfo, cacheDir);
            }

        } else {
            NAMING_LOGGER.info("new ips(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getName() + " -> " + JSON
                .toJSONString(serviceInfo.getHosts()));
            // 新增的instance
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            // 通知服务变更
            eventDispatcher.serviceChanged(serviceInfo);
            serviceInfo.setJsonFromServer(json);
            // 写入磁盘
            DiskCache.write(serviceInfo, cacheDir);
        }

        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());

        NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getName() +
            " -> " + JSON.toJSONString(serviceInfo.getHosts()));

        return serviceInfo;
    }

    private ServiceInfo getServiceInfo0(String serviceName, String clusters) {

        String key = ServiceInfo.getKey(serviceName, clusters);

        return serviceInfoMap.get(key);
    }

    /**
     * 直接从server查询，不经过本地缓存且不注册到server
     */
    public ServiceInfo getServiceInfoDirectlyFromServer(final String serviceName, final String clusters) throws NacosException {
        String result = serverProxy.queryList(serviceName, clusters, 0/*不会注册到server*/, false);
        if (StringUtils.isNotEmpty(result)) {
            return JSON.parseObject(result, ServiceInfo.class);
        }
        return null;
    }

    // 获取serviceInfo
    public ServiceInfo getServiceInfo(final String serviceName, final String clusters) {

        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        String key = ServiceInfo.getKey(serviceName, clusters);
        // 开启故障转移，则从本地缓存获取
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }

        // 从serviceInfoMap中读取
        ServiceInfo serviceObj = getServiceInfo0(serviceName, clusters);

        if (null == serviceObj) {
            // 创建serviceInfo并更新内容
            serviceObj = new ServiceInfo(serviceName, clusters);

            serviceInfoMap.put(serviceObj.getKey(), serviceObj);

            updatingMap.put(serviceName, new Object());
            // 请求API，更新serviceInfo信息
            updateServiceNow(serviceName, clusters);
            updatingMap.remove(serviceName);

        } else if (updatingMap.containsKey(serviceName)) {
            // 正在进行更新操作
            if (UPDATE_HOLD_INTERVAL > 0) {
                // hold a moment waiting for update finish
                synchronized (serviceObj) {
                    try {
                        serviceObj.wait(UPDATE_HOLD_INTERVAL);
                    } catch (InterruptedException e) {
                        NAMING_LOGGER.error("[getServiceInfo] serviceName:" + serviceName + ", clusters:" + clusters, e);
                    }
                }
            }
        }

        // 提交定时更新任务（定时刷新serviceInfo）
        scheduleUpdateIfAbsent(serviceName, clusters);

        return serviceInfoMap.get(serviceObj.getKey());
    }

    public void scheduleUpdateIfAbsent(String serviceName, String clusters) {
        if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
            return;
        }

        synchronized (futureMap) {
            if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
                return;
            }

            // 提交定时更新任务（定时刷新serviceInfo）
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, clusters));
            futureMap.put(ServiceInfo.getKey(serviceName, clusters), future);
        }
    }

    public void updateServiceNow(String serviceName, String clusters) {
        ServiceInfo oldService = getServiceInfo0(serviceName, clusters);
        try {
            // 请求API,获取instance列表(会携带当前客户端的udpPort，用于接收push请求)
            String result = serverProxy.queryList(serviceName, clusters, pushReceiver.getUDPPort(), false);
            if (StringUtils.isNotEmpty(result)) {
                // 解析返回结果
                processServiceJSON(result);
            }
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        } finally {
            if (oldService != null) {
                synchronized (oldService) {
                    oldService.notifyAll();
                }
            }
        }
    }

    public void refreshOnly(String serviceName, String clusters) {
        try {
            serverProxy.queryList(serviceName, clusters, pushReceiver.getUDPPort(), false);
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }

    /**
     * 定时刷新任务
     */
    public class UpdateTask implements Runnable {
        long lastRefTime = Long.MAX_VALUE;
        private String clusters;
        private String serviceName;

        public UpdateTask(String serviceName, String clusters) {
            this.serviceName = serviceName;
            this.clusters = clusters;
        }

        @Override
        public void run() {
            try {
                ServiceInfo serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));

                if (serviceObj == null) {
                    // 更新serviceInfo
                    updateServiceNow(serviceName, clusters);
                    executor.schedule(this, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
                    return;
                }

                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    // 更新service列表
                    updateServiceNow(serviceName, clusters);
                    serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));
                } else {
                    // if serviceName already updated by push, we should not override it
                    // since the push data may be different from pull through force push
                    refreshOnly(serviceName, clusters);
                }

                // 继续调度当前任务，定时刷新本地serviceInfo
                executor.schedule(this, serviceObj.getCacheMillis(), TimeUnit.MILLISECONDS);

                lastRefTime = serviceObj.getLastRefTime();
            } catch (Throwable e) {
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + serviceName, e);
            }

        }
    }
}
