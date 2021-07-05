package com.fahai.cc.cep_stat_pro;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * author:rdx
 * 从目标csv中读取模拟登录的数据，实时检测，如果5秒钟之内连续登录的次数超过2次，则马上告警
 * 1234,10.0.1.1,fail,1611373940
 * long string string long
 */
public class CepOfMaliceLoginDetect {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\faillogin.txt";
        OutputTag<Tuple2<Long,Integer>> outputTag = new OutputTag<Tuple2<Long,Integer>>("tag"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        KeyedStream<Tuple4<Long, String, String, Long>, Long> keyedStream = dataSource.map(new MapFunction<String, Tuple4<Long, String, String, Long>>() {
            @Override
            public Tuple4<Long, String, String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple4<>(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple4<Long, String, String, Long> t, long l) {
                return t.f3 * 1000;
            }
        })).keyBy(f -> f.f0);


        SingleOutputStreamOperator<Tuple4<Long, String, String, Long>> resultSource = keyedStream.process(new MyProcessFunction(outputTag));
        resultSource.getSideOutput(outputTag).print("连续两次登录失败在5s内");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    @Test
    public void CepOfMaliceLogin(){
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\faillogin.txt";
        OutputTag<Tuple2<Long,Integer>> outputTag = new OutputTag<Tuple2<Long,Integer>>("tag"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        KeyedStream<Tuple4<Long, String, String, Long>, Long> keyedStream = dataSource.map(new MapFunction<String, Tuple4<Long, String, String, Long>>() {
            @Override
            public Tuple4<Long, String, String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple4<>(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple4<Long, String, String, Long> t, long l) {
                return t.f3 * 1000;
            }
        })).keyBy(f -> f.f0);

        Pattern<Tuple4<Long, String, String, Long>, Tuple4<Long, String, String, Long>> pattern = Pattern.<Tuple4<Long, String, String, Long>>begin("start").where(new SimpleCondition<Tuple4<Long, String, String, Long>>() {
            @Override
            public boolean filter(Tuple4<Long, String, String, Long> t4) throws Exception {
                if (t4.f2.equals("fail")) {
                    return true;
                }
                return false;
            }
        }).next("second").where(new SimpleCondition<Tuple4<Long, String, String, Long>>() {
            @Override
            public boolean filter(Tuple4<Long, String, String, Long> t4) throws Exception {
                if (t4.f2.equals("fail")) {
                    return true;
                }
                return false;
            }
        }).within(Time.seconds(6));

        PatternStream<Tuple4<Long, String, String, Long>> patternStream = CEP.pattern(keyedStream, pattern);
        patternStream.select(new PatternSelectFunction<Tuple4<Long,String,String,Long>, Tuple4<Long,String,String,Integer>>() {
            @Override
            public Tuple4<Long, String, String, Integer> select(Map<String, List<Tuple4<Long, String, String, Long>>> map) throws Exception {
                Tuple4<Long, String, String, Long> start = map.get("start").get(0);
                Tuple4<Long, String, String, Long> second = map.get("second").iterator().next();
                return new Tuple4<>(start.f0,start.f3.toString(),second.f3.toString(),2);
            }
        }).print("5s内多次登录失败");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
class MyProcessFunction extends KeyedProcessFunction<Long,Tuple4<Long,String,String,Long>,Tuple4<Long,String,String,Long>>{

    private transient ValueState<Long> timeState;
    private transient ValueState<Integer> countState;
    private static final Integer limitNun = 2;
    private OutputTag<Tuple2<Long,Integer>> outputTag;

    public MyProcessFunction(OutputTag<Tuple2<Long,Integer>> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", TypeInformation.of(new TypeHint<Long>() {
        })));
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("countState",TypeInformation.of(new TypeHint<Integer>() {
        })));
    }

    @Override
    public void processElement(Tuple4<Long, String, String, Long> value, Context ctx, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
        Long timeT = timeState.value();
        Integer num = countState.value();
        if(null == num){
            num = 0;
        }
        String status = value.f2;
        if(status.equals("fail")){
            countState.update(num+=1);
            long lastTime = value.f3 + 2000;
            if(null == timeT){

                ctx.timerService().registerEventTimeTimer(lastTime);
                timeState.update(lastTime);
            }
        }
        if(status.equals("success")){
            ctx.timerService().deleteEventTimeTimer(timeState.value());
            timeState.clear();
            countState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
        Integer value = countState.value();
        if(value>=limitNun){
           ctx.output(outputTag,new Tuple2<>(ctx.getCurrentKey(),value));
        }
        timeState.clear();
        countState.clear();
    }

    @Override
    public void close() throws Exception {
        timeState.clear();
        countState.clear();
    }



}