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
package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper implements ServerChangeListener {

    // 当前健康机器地址列表（重复次数代表权重）
    private List<String/*地址*/> healthyList = new ArrayList<>();

    public List<String> getHealthyList() {
        return healthyList;
    }

    @Autowired
    private SwitchDomain switchDomain;

    // 管理server列表
    @Autowired
    private ServerListManager serverListManager;

    /**
     * init server list
     * 将当前对象作为listener，添加到
     */
    @PostConstruct
    public void init() {
        serverListManager.listen(this);
    }

    /**
     * 检查是否由本地server响应该节点
     * @return
     */
    public boolean responsible(Cluster cluster, Instance instance) {
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName())/*开启了健康状况检查*/
            && !cluster.getHealthCheckTask().isCancelled()/*检查任务未取消*/
            && responsible(cluster.getServiceName())/*可由本地server响应服务*/
            && cluster.contains(instance);/*实例在集群内仍存活*/
    }

    /**
     * 是否由本地server响应该服务
     */
    public boolean responsible(String serviceName) {
        // 开关关闭 或 单机模式
        if (!switchDomain.isDistroEnabled() || SystemUtils.STANDALONE_MODE) {
            return true;
        }

        // 当前健康机器为空
        if (CollectionUtils.isEmpty(healthyList)) {
            // means distro config is not ready yet
            return false;
        }

        int index = healthyList.indexOf(NetUtils.localServer());
        int lastIndex = healthyList.lastIndexOf(NetUtils.localServer());
        if (lastIndex < 0 || index < 0) {
            return true;
        }

        int target = distroHash(serviceName) % healthyList.size();
        return target >= index && target <= lastIndex;
    }

    /**
     * 根据service查找server地址
     * @param serviceName
     * @return
     */
    public String mapSrv(String serviceName) {
        // 如果健康节点为空那个 或 开关关闭，返回本地地址
        if (CollectionUtils.isEmpty(healthyList) || !switchDomain.isDistroEnabled()) {
            return NetUtils.localServer();
        }

        // hash算法，获取一个节点
        try {
            return healthyList.get(distroHash(serviceName) % healthyList.size());
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("distro mapper failed, return localhost: " + NetUtils.localServer(), e);

            return NetUtils.localServer();
        }
    }

    public int distroHash(String serviceName) {
        return Math.abs(serviceName.hashCode() % Integer.MAX_VALUE);
    }

    @Override
    public void onChangeServerList(List<Server> latestMembers) {

    }

    /**
     * 更新健康节点列表
     * @param latestReachableMembers
     */
    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

        List<String> newHealthyList = new ArrayList<>();
        for (Server server : latestReachableMembers) {
            newHealthyList.add(server.getKey());
        }
        healthyList = newHealthyList;
    }
}
