package com.fahai.cc.kafka_api;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

/**
 * 项目：flink_pro
 * 包名：com.fahai.cc.kafka_api
 * 作者：rdx
 * 日期：2021/7/29 9:30
 * brocker:cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092
 *描述：不带回调函数kafka生产者
 */
public class MyProducer {
    /**
     * 不带回调函数kafka生产者
     * @param args
     */
    public static void main(String[] args) {
        Properties prop = new Properties();
        /*broker*/
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092");
        /*消息确认机制*/
        prop.put(ProducerConfig.ACKS_CONFIG,"all");
        /*key,value序列化与反序列化*/
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        /**
         * 以下配置为可配置选项
         */
        /*重试次数*/
        prop.put(ProducerConfig.RETRIES_CONFIG,"3");
        /*批次大小*/
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        /*等待时间/延迟时间，默认无延迟*/
        prop.put(ProducerConfig.LINGER_MS_CONFIG,1);
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        for(int i=0;i<10;i++){
            /*指定分区进行数据的发送*/
//            producer.send(new ProducerRecord<String, String>("test_r",0,"rdx_"+i,"hello:"+i));
            /*不指定特定的分区写入数据，直接发送value，以轮询的方式发送消息*/
//            producer.send(new ProducerRecord<String, String>("test_r","hahaha:" + i));
            /*不指定特定分区写入数据，根据key进行hash % partition 将数据发送到相应的分区中*/
            producer.send(new ProducerRecord<String, String>("test_r","rdx_"+3,"lalal_"+i));
        }

        producer.close();
    }

    /**
     * 生产者回调函数的使用
     * CallBack
     */
    @Test
    public void testCallBack(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092");
        prop.put(ProducerConfig.ACKS_CONFIG,"all");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        //添加自定义分区配置
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.fahai.cc.kafka_api.util.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        for(int i=0;i<10;i++){
            producer.send(new ProducerRecord<String, String>("test_r", "a_" + i, "haha_" + i), (recordMetadata, e) -> {
                //回调函数可以获取相应的topic,partition,offset等
                if(e == null){
                    System.out.println(recordMetadata.partition() + "_" +recordMetadata.offset());
                }
            });
        }

        producer.close();
    }
}
