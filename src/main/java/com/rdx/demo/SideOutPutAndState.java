package com.rdx.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * 项目：flink_pro
 * 包名：com.rdx.demo
 * 作者：rdx
 * 日期：2021/7/21 10:14
 */
public class SideOutPutAndState {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        OutputTag<Tuple2<String,String>> outputTag1 = new OutputTag<Tuple2<String, String>>("tag1"){};
        OutputTag<Tuple2<String,String>> outputTag2 = new OutputTag<Tuple2<String, String>>("tag2"){};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultSource = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] line = s.split(" ");
                for (String ss : line) {
                    collector.collect(new Tuple2<>(ss, 1));
                }
            }
        }).keyBy(f -> f.f0)
                .process(new MyProcessFunction(outputTag1,outputTag2));
        resultSource.getSideOutput(outputTag1).print("奇数");
        resultSource.getSideOutput(outputTag2).print("偶数");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static class MyProcessFunction extends KeyedProcessFunction<String, Tuple2<String,Integer>, Tuple2<String,Integer>>{

        private transient ValueState<Integer> countState;

        private OutputTag<Tuple2<String,String>>  outputTag1;
        private OutputTag<Tuple2<String,String>>  outputTag2;

        public MyProcessFunction(OutputTag<Tuple2<String, String>> outputTag1, OutputTag<Tuple2<String, String>> outputTag2) {
            this.outputTag1 = outputTag1;
            this.outputTag2 = outputTag2;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("countState", TypeInformation.of(new TypeHint<Integer>() {
            })));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer countSt = countState.value();
            if(countSt == null){
                countSt = 0;
            }
            Integer count = value.f1+ countSt;
            if(count%2 == 1){
                ctx.output(outputTag1,new Tuple2<>(ctx.getCurrentKey(),"统计结果值：" + count + " 为奇数" ));
                countState.update(count);
            }
            if(count %2 == 0){
                ctx.output(outputTag2,new Tuple2<>(ctx.getCurrentKey(),"统计结果值：" + count + " 为偶数" ));
                countState.update(count);
            }

        }

        @Override
        public void close() throws Exception {
            countState.clear();
        }
    }
}
