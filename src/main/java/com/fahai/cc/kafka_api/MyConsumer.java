package com.fahai.cc.kafka_api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * 项目：flink_pro
 * 包名：com.fahai.cc.kafka_api
 * 作者：rdx
 * 日期：2021/7/30 15:32
 * kafka消费者
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        String topic = "test_r";
        //kafka集群
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092");
       //消费者组
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //序列化与反序列化
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        //自动提交
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //重置offset
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //创建kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        //订阅主题
        consumer.subscribe(Collections.singleton(topic));
        while(true){
            ConsumerRecords<String, String> pollData = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : pollData){
                System.out.println("key:"+record.key() +",offset:"+ record.offset() +",partition:"+ record.partition() +",value:"+ record.value());
            }

        }

    }
}
