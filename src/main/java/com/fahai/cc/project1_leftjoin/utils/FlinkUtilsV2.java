package com.fahai.cc.project1_leftjoin.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class FlinkUtilsV2 {
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 方法重载一：传入配置文件和数据类型
    public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameters,
                                                          Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        String topics = parameters.getRequired("kafka.topics");
        String groupId = parameters.getRequired("group.id");
        return createKafkaDataStream(parameters, topics, groupId, clazz);
    }


    // 方法重载二：传入配置文件和数据类型
    public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameters, String topics,
                                                          Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        String groupId = parameters.getRequired("group.id");
        return createKafkaDataStream(parameters, topics, groupId, clazz);
    }


    // 方法重载一：传入配置文件和数据类型
    public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameters, String topics, String groupId,
                                                          Class<? extends DeserializationSchema<T>> clazz) throws Exception {

        // 将配置文件设定为全局配置文件
        env.getConfig().setGlobalJobParameters(parameters);


        //  KafkaSink 需要配置这个
        //parameters.getProperties().setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");

        //开启checkpoint
        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 10000L),
                CheckpointingMode.EXACTLY_ONCE);

        //设定重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                parameters.getInt("restart.times", 10), Time.seconds(3)));

        //设置statebackend
        String path = parameters.get("state.backend.path");
        if (path != null) {
            //最好的方式将setStateBackend配置到Flink的全局配置文件中flink-conf.yaml
            StateBackend fsStateBackend = new FsStateBackend(path);
            env.setStateBackend(fsStateBackend);
        }

        //设置cancel任务不自动删除checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        //设定最大的并行度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(4);

        //当出现多个topic时，放在list集合中
        List<String> topicList = Arrays.asList(topics.split(","));


        Properties properties = parameters.getProperties();

        properties.setProperty("group.id", groupId);

        //创建FlinkKafkaConsumer
        FlinkKafkaConsumer011<T> kafkaConsumer = new FlinkKafkaConsumer011<T>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //在Checkpoint的时候将Kafka的偏移量保存到Kafka特殊的Topic中，默认是true
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        // 返回kafkaDataStream (lines)
        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
