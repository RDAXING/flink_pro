package com.fahai.cc.cep_stat_pro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class StatOfCheckIP {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        OutputTag<Tuple2<String,String>> outputTag = new OutputTag<Tuple2<String,String>>("tag"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\cep.txt";
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");

        SingleOutputStreamOperator<String> resultSource = dataSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[1], s);
            }
        }).keyBy(f -> f.f0).process(new MyKeyedProcessFunction(outputTag));

        resultSource.getSideOutput(outputTag).print("outputTag");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MyKeyedProcessFunction extends  KeyedProcessFunction<String, Tuple2<String,String>, String>{

    private transient ValueState<Tuple2<String,String>> ipStat;
    private OutputTag<Tuple2<String,String>> outputTag ;

    public MyKeyedProcessFunction(OutputTag<Tuple2<String, String>> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ipStat = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, String>>("ipStat", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        })));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
        Tuple2<String, String> lastIp = ipStat.value();
        String ips = value.f1.split(",")[0];
        if(lastIp == null){
            lastIp = new Tuple2(value.f0,ips);
        }

        if(!lastIp.f1.equals(ips)){
            ctx.output(outputTag,value);
        }

        ipStat.update(new Tuple2<>(value.f0,ips));
    }

    @Override
    public void close() throws Exception {
        ipStat.clear();
    }
}