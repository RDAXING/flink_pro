package com.fahai.cc.kafka_api.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 项目：flink_pro
 * 包名：com.fahai.cc.kafka_api.util
 * 作者：rdx
 * 日期：2021/7/30 15:11
 * 描述：自定义分区器
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes1, Cluster cluster) {
        Integer integer = cluster.partitionCountForTopic(topic);
//        return 2;
        return key.toString().hashCode() % integer;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
