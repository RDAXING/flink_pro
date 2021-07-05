package customSource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class FlinkReadKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "CRV_ES";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group_fk");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        DataStreamSource<String> kafkaDataSource = env.addSource(new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties));
        kafkaDataSource.print();
        try {
            env.execute("kafkaJob");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
