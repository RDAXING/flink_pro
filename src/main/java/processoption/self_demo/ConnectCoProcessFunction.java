package processoption.self_demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 说明：
 * 获取不同流数据，通过connect进行连接，通过process算子进行具体业务逻辑处理
 * 步骤：
 * 1.先创建两个状态stat1，stat2（如果只接受到一个流的数据，则将当前的value保存到状态中）
 *      获取相同key的value值进行相加（当两个流都获取到数据）
 * 2.定时器的创建 timerStat
 *      当其中有一个没有接受到对应的数据流，并且超时10s就触发定时器，如果在10s内接受到对应的数据就删除定时器
 * 3.测流输出
 *    对超时10s没有接受到的数据就进行测流输出
 *
 */
public class ConnectCoProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OutputTag<String> outputTag1 = new OutputTag<String>("tag1"){};
        OutputTag<String> outputTag2 = new OutputTag<String>("tag2"){};
        KeyedStream<Tuple2<String, Integer>, String> source1 = getSocketSource("localhost", 6666, env);
        KeyedStream<Tuple2<String, Integer>, String> source2 = getSocketSource("localhost", 7777, env);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = source1.connect(source2).process(new MyConnectProcessFunction(outputTag1, outputTag2));
        resultStream.print();
        resultStream.getSideOutput(outputTag1).print();
        resultStream.getSideOutput(outputTag2).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KeyedStream<Tuple2<String, Integer>, String> getSocketSource(String ip , int port, StreamExecutionEnvironment env){
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream(ip, port);
        KeyedStream<Tuple2<String, Integer>, String> resSource = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if (StringUtils.isBlank(s) || s.split(",").length != 2) {
                    return null;
                }
                return new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
            }
        }).keyBy(f -> f.f0);

        return  resSource;
    }



}

class MyConnectProcessFunction extends CoProcessFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>{
    private OutputTag<String> outputTag1;
    private OutputTag<String> outputTag2;
    private ValueState<Integer> stat1;
    private ValueState<Integer> stat2;
    private ValueState<String> currentKey;
    private ValueState<Long> timerStat;
    private static final Integer interval = 10;
    public MyConnectProcessFunction(OutputTag<String> outputTag1, OutputTag<String> outputTag2) {
        this.outputTag1 = outputTag1;
        this.outputTag2 = outputTag2;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        stat1 = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("stat1", TypeInformation.of(new TypeHint<Integer>() {
        })));
        stat2 = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("stat2", TypeInformation.of(new TypeHint<Integer>() {
        })));

        timerStat = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerStat", TypeInformation.of(new TypeHint<Long>() {
        })));
        currentKey = getRuntimeContext().getState(new ValueStateDescriptor<String>("currentKeyStat", TypeInformation.of(new TypeHint<String>() {
        })));

    }

    @Override
    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

        Integer value2 = stat2.value();

        if(value2 == null){
            System.out.println("2号流：未接受到数值，将1号流："+value.f0+"_"+value.f1+"保存到状态中");
            stat1.update(value.f1);
            System.out.println("2号流：未接受到数值，超过10s,触发定时器");
            long timerTs = ctx.timerService().currentProcessingTime() + interval * 1000;
            ctx.timerService().registerProcessingTimeTimer(timerTs);
            timerStat.update(timerTs);
            currentKey.update(value.f0);
        }else{
            System.out.println("2号流：接受到数值 "+value2+"，将1号流："+value.f0+"_"+value.f1+"中的value与2号value 相加");
            value.f1 += value2;
            out.collect(new Tuple2<>(value.f0,value.f1));
            System.out.println("2号流：接受到数值，删除触发定时器");
            ctx.timerService().deleteProcessingTimeTimer(timerStat.value());
            stat2.clear();
            timerStat.clear();
            currentKey.clear();
            stat1.clear();
        }
    }

    @Override
    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = stat1.value();
        if(value1 == null){
            System.out.println("1号流：未接受到数值，将2号流："+value.f0+"_"+value.f1+"保存到状态中");
            stat2.update(value.f1);
            System.out.println("1号流：未接受到数值，超过10s,触发定时器");
            long timeTs = ctx.timerService().currentProcessingTime() + interval * 1000;
            ctx.timerService().registerProcessingTimeTimer(timeTs);
            timerStat.update(timeTs);
            currentKey.update(value.f0);
        }else{
            System.out.println("1号流：接受到数值 "+value1+"，将2号流："+value.f0+"_"+value.f1+"中的value与1号value 相加");
            value.f1+=value1;
            out.collect(new Tuple2<>(value.f0,value.f1));
            System.out.println("1号流：接受到数值，删除触发定时器");
            ctx.timerService().deleteProcessingTimeTimer(timerStat.value());
            stat1.clear();
            timerStat.clear();
            stat2.clear();
            currentKey.clear();

        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = stat1.value();
        Integer value2 = stat2.value();
        if(value1 != null){
            ctx.output(outputTag1,"触发定时："+currentKey.value()+"_"+value1);
            currentKey.clear();
            stat1.clear();
            timerStat.clear();
        }
        if(value2 != null){
            ctx.output(outputTag2,"触发定时："+currentKey.value()+"_"+value2);
            stat2.clear();
            timerStat.clear();
            currentKey.clear();
        }
    }

    @Override
    public void close() throws Exception {
        stat2.clear();
        stat1.clear();
        timerStat.clear();
        currentKey.clear();
    }
}
