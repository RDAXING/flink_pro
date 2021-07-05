package com.fahai.cc.cep_stat_pro;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

public class CepOfTemperature {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\temp.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        KeyedStream<Tuple5<String, String, String, String, String>, String> keyedStream = dataSource.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String s) throws Exception {
                String[] split = s.split(",");

                return new Tuple5<>(split[0], split[1], split[2], split[3], split[4]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple5<String, String, String, String, String>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple5<String, String, String, String, String>>() {
            @Override
            public long extractTimestamp(Tuple5<String, String, String, String, String> t, long l) {

                FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                long times = 0l;
                try {
                    times = format.parse(t.f4).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return times;
            }
        })).keyBy(f -> f.f0);

        Pattern<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>> patterns = Pattern.<Tuple5<String, String, String, String, String>>begin("start").where(new SimpleCondition<Tuple5<String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple5<String, String, String, String, String> t) throws Exception {
                if (Double.parseDouble(t.f2) >= 40) {
                    return true;
                }
                return false;
            }
        }).followedBy("second").where(new SimpleCondition<Tuple5<String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple5<String, String, String, String, String> t) throws Exception {
                if (Double.parseDouble(t.f2) >= 40) {
                    return true;
                }
                return false;
            }
        }).followedBy("third").where(new SimpleCondition<Tuple5<String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple5<String, String, String, String, String> t) throws Exception {
                if (Double.parseDouble(t.f2) >= 40) {
                    return true;
                }
                return false;
            }
        }).within(Time.minutes(3));

        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> select = CEP.pattern(keyedStream, patterns).select(new PatternSelectFunction<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> select(Map<String, List<Tuple5<String, String, String, String, String>>> map) throws Exception {

                Tuple5<String, String, String, String, String> start = map.get("start").iterator().next();
                System.out.println("第一次比对：" + start);
                Tuple5<String, String, String, String, String> second = map.get("second").iterator().next();
                System.out.println("第二次比对：" + second);
                Tuple5<String, String, String, String, String> third = map.get("third").iterator().next();
                System.out.println("第三次比对：" + third);

                return third;
            }
        });

        select.print("temperature 大于 40");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
