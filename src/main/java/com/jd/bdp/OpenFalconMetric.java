package com.jd.bdp;

/**
 * Created by tangshangwen on 17-3-6.
 */
public class OpenFalconMetric {

    /**
     * 标明Metric的主体
     */
    private String endpoint;

    /**
     * 采集项具体度量
     */
    private String metric;

    /**
     * 表示汇报该数据时的unix时间戳，注意是整数，代表的是秒
     */
    private Long timestamp;


    /**
     * 表示该数据采集项的汇报周期
     */
    private int step;

    /**
     * 代表该metric在当前时间点的值
     */
    private Object value;

    /**
     * 只能是COUNTER或者GAUGE二选一
     * 前者表示该数据采集项为计时器类型，后者表示其为原值 (注意大小写)
     * GAUGE：即用户上传什么样的值，就原封不动的存储
     * COUNTER：指标在存储和展现的时候，会被计算为speed，即（当前值 - 上次值）/ 时间间隔
     */
    private String counterType = "GAUGE";

    /**
     * 一组逗号分割的键值对, 对metric进一步描述和细化,
     * 可以是空字符串. 比如idc=lg，比如service=xbox等，多个tag之间用逗号分割
     */
    private String tags="";

    private OpenFalconMetric() {
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    public static class Builder {
        private final OpenFalconMetric metric;

        public Builder(String name) {
            this.metric = new OpenFalconMetric();
            this.metric.metric = name;
        }

        public OpenFalconMetric build() {
            return metric;
        }

        public Builder withValue(Object value) {
            metric.value = value;
            return this;
        }

        public Builder withEndpoint(String endpoint) {
            metric.endpoint = endpoint;
            return this;
        }

        public Builder withStep(int step) {
            metric.step = step;
            return this;
        }

        public Builder withCounterType(String counterType) {
            metric.counterType = counterType;
            return this;
        }

        public Builder withTimestamp(Long timestamp) {
            metric.timestamp = timestamp;
            return this;
        }

        public Builder withTags(String tags) {
            metric.tags = tags;
            return this;
        }
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public String getTags() {
        return tags;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getMetric() {
        return metric;
    }

    public int getStep() {
        return step;
    }

    public String getCounterType() {
        return counterType;
    }

    private boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

}

/*
详细请看
https://book.open-falcon.org/zh/usage/data-push.html

#!-*- coding:utf8 -*-

import requests
import time
import json

ts = int(time.time())
payload = [
    {
        "endpoint": "test-endpoint",
        "metric": "test-metric",
        "timestamp": ts,
        "step": 60,
        "value": 1,
        "counterType": "GAUGE",
        "tags": "idc=lg,loc=beijing",
    },

    {
        "endpoint": "test-endpoint",
        "metric": "test-metric2",
        "timestamp": ts,
        "step": 60,
        "value": 2,
        "counterType": "GAUGE",
        "tags": "idc=lg,loc=beijing",
    },
]

r = requests.post("http://127.0.0.1:1988/v1/push", data=json.dumps(payload))

print r.text

*/
