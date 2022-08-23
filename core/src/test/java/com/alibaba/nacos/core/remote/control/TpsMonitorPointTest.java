/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.remote.control;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link TpsMonitorPoint} unit tests.
 *
 * @author chenglu
 * @date 2021-06-18 14:02
 */
public class TpsMonitorPointTest {
    
    @Test
    public void testStaticMethod() {
        long current = System.currentTimeMillis();
        String date = TpsMonitorPoint.getTimeFormatOfSecond(current);
        Assert.assertNotNull(date);

        long trimMillsOfSecond = TpsMonitorPoint.getTrimMillsOfSecond(current);
        Assert.assertEquals(current / 1000, trimMillsOfSecond / 1000);
        Assert.assertEquals(0, trimMillsOfSecond % 1000);

        long trimMillsOfMinute = TpsMonitorPoint.getTrimMillsOfMinute(current);
        Assert.assertEquals(trimMillsOfSecond / 1000 / 60, trimMillsOfMinute / 1000 / 60);
        Assert.assertEquals(0, trimMillsOfMinute % (1000 * 60));

        long trimMillsOfHour = TpsMonitorPoint.getTrimMillsOfHour(current);
        Assert.assertEquals(current / 1000 / 60 / 60, trimMillsOfHour / 1000 / 60 / 60);
        Assert.assertEquals(0, trimMillsOfHour % (1000 * 60 * 60));
    }
}
