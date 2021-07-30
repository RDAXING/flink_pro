package com.fahai.cc.kafka_api;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * 项目：flink_pro
 * 包名：com.fahai.cc.kafka_api
 * 作者：rdx
 * 日期：2021/7/30 16:08
 */
public class KafkaConsumeLagMonitor {

    public static Properties getConsumeProperties(String groupID, String bootstrap_server) {
        Properties props = new Properties();
        props.put("group.id", groupID);
        props.put("bootstrap.servers", bootstrap_server);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    //"REPORT_CACHE_NORMAL", "REPORT_CACHE_CONSUMER"
    public static void main(String[] args) {
        String bootstrap_server = "cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092";
        String groupID = "REPORT_CACHE_CONSUMER";
        String topic = "REPORT_CACHE_NORMAL";
        Map<Integer, Long> endOffsetMap = new HashMap<Integer, Long>();
        Map<Integer, Long> commitOffsetMap = new HashMap<Integer, Long>();

        Properties consumeProps = getConsumeProperties(groupID, bootstrap_server);
        System.out.println("consumer properties:" + consumeProps);
        //查询topic partitions
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumeProps);
        List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionsFor) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        }

        //查询log size
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        for (TopicPartition partitionInfo : endOffsets.keySet()) {
            endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
        }
        for (Integer partitionId : endOffsetMap.keySet()) {
            System.out.println(String.format("at %s, topic:%s, partition:%s, logSize:%s", System.currentTimeMillis(), topic, partitionId, endOffsetMap.get(partitionId)));
        }

        //查询消费offset
        for (TopicPartition topicAndPartition : topicPartitions) {
            OffsetAndMetadata committed = consumer.committed(topicAndPartition);
            commitOffsetMap.put(topicAndPartition.partition(), committed.offset());
        }

        //累加lag
        long lagSum = 0l;
        if (endOffsetMap.size() == commitOffsetMap.size()) {
            for (Integer partition : endOffsetMap.keySet()) {
                long endOffSet = endOffsetMap.get(partition);
                long commitOffSet = commitOffsetMap.get(partition);
                long diffOffset = endOffSet - commitOffSet;
                lagSum += diffOffset;
                System.out.println("Topic:" + topic + ", groupID:" + groupID + ", partition:" + partition + ", endOffset:" + endOffSet + ", commitOffset:" + commitOffSet + ", diffOffset:" + diffOffset);
            }
            System.out.println("Topic:" + topic + ", groupID:" + groupID + ", LAG:" + lagSum);
        } else {
            System.out.println("this topic partitions lost");
        }

        consumer.close();
    }


}
