package com.rdx.demo;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 项目：flink_pro
 * 包名：com.rdx.demo
 * 作者：rdx
 * 日期：2021/7/21 10:54
 */
public class TempTenUpWarn {
    public static void main(String[] args) {
        OutputTag<Tuple3<String,String,String>> outputTag = new OutputTag<Tuple3<String, String, String>>("tag"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取配置文件中的数据
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();
        Properties properties = parameterTool.getProperties();
        String property = properties.getProperty("mysql.url");
        System.out.println(property);
        //设置全局参数 ,可以在自定义函数中使用
        env.getConfig().setGlobalJobParameters(ExecutionEnvUtil.createParameterTool());
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple3<String, Long, String>> resultSource = dataSource.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] line = s.split(",");
                return new Tuple3<>(line[0], Long.parseLong(line[1]), Double.parseDouble(line[2]));
            }
        }).uid("map-id").keyBy(f -> f.f0).process(new MyKeyedProcessFuntion(outputTag)).uid("process-id");

        resultSource.print("正常数据");
        resultSource.getSideOutput(outputTag).print("warn");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static class MyKeyedProcessFuntion extends  KeyedProcessFunction<String, Tuple3<String, Long, Double>, Tuple3<String, Long, String>>{

        private OutputTag<Tuple3<String,String,String>> outputTag;

        public MyKeyedProcessFuntion(OutputTag<Tuple3<String, String, String>> outputTag) {
            this.outputTag = outputTag;
        }

        private transient ValueState<Double> tempState;
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取全局配置文件数据
            ParameterTool prop = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Properties properties = prop.getProperties();
            String property = properties.getProperty("mysql.url");
            System.out.println(property);

            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", TypeInformation.of(new TypeHint<Double>() {
            })));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer",TypeInformation.of(new TypeHint<Long>() {
            })));
        }

        @Override
        public void processElement(Tuple3<String, Long, Double> value, Context ctx, Collector<Tuple3<String, Long, String>> out) throws Exception {
            Double lastTemp = tempState.value();
            Long currentTimer = timerState.value();
            if(lastTemp == null){
                lastTemp = value.f2;
            }

            if(value.f2 > lastTemp && currentTimer == null){
                long ts = ctx.timerService().currentProcessingTime() + 5000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerState.update(ts);
            }

            if(value.f2 < lastTemp && currentTimer != null){
                ctx.timerService().deleteProcessingTimeTimer(timerState.value());
                timerState.clear();
            }
            tempState.update(value.f2);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Long, String>> out) throws Exception {
            Long ts = timerState.value();
            if(ts != null){
                ctx.output(outputTag,new Tuple3<>(ctx.getCurrentKey(),new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp)),"10s内温度连续上升"));
                timerState.clear();
            }
        }

        @Override
        public void close() throws Exception {
            timerState.clear();
            tempState.clear();
        }
    }


    @Test
    public void CepOfTempTenUp(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Double>> mapSource = dataSource.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String s) throws Exception {
                String[] line = s.split(",");
                return new Tuple2<>(line[0], Double.parseDouble(line[1]));
            }
        });

        Pattern<Tuple2<String, Double>, Tuple2<String, Double>> withinPattern = Pattern.<Tuple2<String, Double>>begin("start").where(new SimpleCondition<Tuple2<String, Double>>() {
            @Override
            public boolean filter(Tuple2<String, Double> t2) throws Exception {
                if (t2.f1 > 30) {
                    return true;
                }
                return false;
            }
        }).next("second").where(new IterativeCondition<Tuple2<String, Double>>() {
            @Override
            public boolean filter(Tuple2<String, Double> t2, Context<Tuple2<String, Double>> context) throws Exception {

                if(t2.f1 > 30){
                    return true;
                }


                return false;
            }
        }).within(Time.seconds(5));


        PatternStream<Tuple2<String, Double>> pattern = CEP.pattern(mapSource.keyBy(f -> f.f0), withinPattern);

        SingleOutputStreamOperator<Tuple2<String, Double>> select = pattern.select(new PatternSelectFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> select(Map<String, List<Tuple2<String, Double>>> map) throws Exception {
                Tuple2<String, Double> start = map.get("start").iterator().next();
                Tuple2<String, Double> second = map.get("second").iterator().next();
                System.out.println("第一次温度：" + start);
                System.out.println("第二次温度：" + second);

                return second;
            }
        });


        select.print("cep");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
