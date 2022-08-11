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

package com.alibaba.nacos.naming.web;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to determine if method should be redirected.
 * 标识请求是否可以重定向到其他节点处理 {@link DistroFilter}
 * ps.会根据请求的ip、port或者serviceName 的hashCode决定处理的节点
 *
 * @author nkorange
 * @since 1.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CanDistro {
    
}
