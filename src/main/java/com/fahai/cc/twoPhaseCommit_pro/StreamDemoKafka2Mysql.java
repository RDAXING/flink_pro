package com.fahai.cc.twoPhaseCommit_pro;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * 消费kafka消息，sink(自定义)到mysql中，保证kafka to mysql的Exactly-Once
 */
@SuppressWarnings("all")
public class StreamDemoKafka2Mysql {

    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        env.setStateBackend(new FsStateBackend("hdfs://cdh2.fahaicc.com:8020/tmp/rdx/cp"));
//        env.setStateBackend(new FsStateBackend("file:///Users/temp/cp/"));
        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_one");
        //kafka分区自动发现周期
        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
        // FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        FlinkKafkaConsumer011<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer011<>("BATCH_ES", new JSONKeyValueDeserializationSchema(true), props);

        //加入kafka数据源
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer011);
        //数据传输到下游
        streamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink");
        //触发执行
        env.execute(StreamDemoKafka2Mysql.class.getName());

    }
}