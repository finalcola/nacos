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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Data sync task dispatcher
 * 分发数据同步任务
 * 内部保存n个任务调度器（数量为CPU个数），提交的任务会被均匀分发到调度器
 * @author nkorange
 * @since 1.0.0
 */
@Component
public class TaskDispatcher {

    @Autowired
    private GlobalConfig partitionConfig;

    /**
     * 数据同步组件
     */
    @Autowired
    private DataSyncer dataSyncer;

    private List<TaskScheduler> taskSchedulerList = new ArrayList<>();

    // CPU个数
    private final int cpuCoreCount = Runtime.getRuntime().availableProcessors();

    @PostConstruct
    public void init() {
        for (int i = 0; i < cpuCoreCount; i++) {
            TaskScheduler taskScheduler = new TaskScheduler(i);
            taskSchedulerList.add(taskScheduler);
            GlobalExecutor.submitTaskDispatch(taskScheduler);
        }
    }

    public void addTask(String key) {
        taskSchedulerList.get(UtilsAndCommons.shakeUp(key, cpuCoreCount)).addTask(key);
    }

    public class TaskScheduler implements Runnable {

        private int index;

        // 当前需要同步的数据数量（用于批处理）
        private int dataSize = 0;

        // 上次提交任务的时间（用于批处理）
        private long lastDispatchTime = 0L;

        private BlockingQueue<String> queue = new LinkedBlockingQueue<>(128 * 1024);

        public TaskScheduler(int index) {
            this.index = index;
        }

        // 添加任务
        public void addTask(String key) {
            queue.offer(key);
        }

        public int getIndex() {
            return index;
        }

        @Override
        public void run() {

            List<String> keys = new ArrayList<>();
            while (true) {

                try {
                    // 从任务队列拉取任务
                    String key = queue.poll(partitionConfig.getTaskDispatchPeriod(),
                        TimeUnit.MILLISECONDS);

                    if (Loggers.EPHEMERAL.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                        Loggers.EPHEMERAL.debug("got key: {}", key);
                    }

                    // server列表为空
                    if (dataSyncer.getServers() == null || dataSyncer.getServers().isEmpty()) {
                        continue;
                    }

                    if (StringUtils.isBlank(key)) {
                        continue;
                    }

                    if (dataSize == 0) {
                        keys = new ArrayList<>();
                    }

                    keys.add(key);
                    dataSize++;

                    // 批处理或超时
                    if (dataSize == partitionConfig.getBatchSyncKeyCount() ||
                        (System.currentTimeMillis() - lastDispatchTime) > partitionConfig.getTaskDispatchPeriod()) {

                        // 提交任务：向其它server同步数据
                        for (Server member : dataSyncer.getServers()) {
                            if (NetUtils.localServer().equals(member.getKey())) {
                                continue;
                            }
                            SyncTask syncTask = new SyncTask();
                            syncTask.setKeys(keys);
                            syncTask.setTargetServer(member.getKey());

                            if (Loggers.EPHEMERAL.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                                Loggers.EPHEMERAL.debug("add sync task: {}", JSON.toJSONString(syncTask));
                            }

                            dataSyncer.submit(syncTask, 0);
                        }
                        lastDispatchTime = System.currentTimeMillis();
                        dataSize = 0;
                    }

                } catch (Exception e) {
                    Loggers.EPHEMERAL.error("dispatch sync task failed.", e);
                }
            }
        }
    }
}
