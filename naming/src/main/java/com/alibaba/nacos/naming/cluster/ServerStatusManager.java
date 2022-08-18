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

import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.common.utils.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Optional;

/**
 * Detect and control the working status of local server.
 *
 * @author nkorange
 * @since 1.0.0
 */
@Service
public class ServerStatusManager {
    
    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;
    
    private final SwitchDomain switchDomain;
    
    private ServerStatus serverStatus = ServerStatus.STARTING;
    
    public ServerStatusManager(SwitchDomain switchDomain) {
        this.switchDomain = switchDomain;
    }
    
    @PostConstruct
    public void init() {
        // 定时更新server状态
        GlobalExecutor.registerServerStatusUpdater(new ServerStatusUpdater());
    }
    
    private void refreshServerStatus() {
        
        if (StringUtils.isNotBlank(switchDomain.getOverriddenServerStatus())) {
            serverStatus = ServerStatus.valueOf(switchDomain.getOverriddenServerStatus());
            return;
        }

        // 检查同步协议是否初始化成功（临时数据和持久化数据协议都初始化成功）
        // 临时数据协议：distro协议，从其他server同步数据成功
        // 持久化数据：raft协议，leader选举成功
        if (consistencyService.isAvailable()) {
            serverStatus = ServerStatus.UP;
        } else {
            serverStatus = ServerStatus.DOWN;
        }
    }
    
    public ServerStatus getServerStatus() {
        return serverStatus;
    }
    
    public Optional<String> getErrorMsg() {
        return consistencyService.getErrorMsg();
    }
    
    public class ServerStatusUpdater implements Runnable {
        
        @Override
        public void run() {
            refreshServerStatus();
        }
    }
}
