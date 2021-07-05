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
 * 两个连接流进行相同key进行求和
 */
public class TwoStreamConnectAPI {
    public static void main(String[] args) {
        OutputTag<String> outputTag1 = new OutputTag<String>("tag1") {
        };
        OutputTag<String> outputTag2 = new OutputTag<String>("tag2") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<String, Integer>, String> source1 = getSocketStream("localhost", 6666, env);
        KeyedStream<Tuple2<String, Integer>, String> source2 = getSocketStream("localhost", 7777, env);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = source1.connect(source2).process(new MyCoProcessFunction(outputTag1,outputTag2));
        resultStream.print();

        resultStream.getSideOutput(outputTag1).print("tag1");
        resultStream.getSideOutput(outputTag2).print("tag2");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KeyedStream<Tuple2<String, Integer>, String> getSocketStream(String ip , Integer port, StreamExecutionEnvironment env){
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream(ip, port);
        KeyedStream<Tuple2<String, Integer>, String> source = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if (StringUtils.isBlank(s) || s.split(",").length != 2) {
                    return null;
                }
                String[] ss = s.split(",");
                return new Tuple2<>(ss[0], Integer.parseInt(ss[1]));
            }
        }).keyBy(f -> f.f0);

        return source;
    }
}


class MyCoProcessFunction extends CoProcessFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>{
    private transient ValueState<Integer> source1Stat ;
    private transient ValueState<Integer> source2Stat ;
    private transient ValueState<Long> timerStat;
    private transient ValueState<String> currentKeyStat;
    private static final Integer interval = 10000;

    private OutputTag<String> outputTag1;

    public MyCoProcessFunction(OutputTag<String> outputTag1, OutputTag<String> outputTag2) {
        this.outputTag1 = outputTag1;
        this.outputTag2 = outputTag2;
    }

    private OutputTag<String> outputTag2;
    @Override
    public void open(Configuration parameters) throws Exception {
        source1Stat = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("soure1Stat", TypeInformation.of(new TypeHint<Integer>() {
        })));
        source2Stat = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("soure2Stat", TypeInformation.of(new TypeHint<Integer>() {
        })));
        timerStat = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerStat",TypeInformation.of(new TypeHint<Long>() {
        })));
        currentKeyStat = getRuntimeContext().getState(new ValueStateDescriptor<String>("currentKeyStat",TypeInformation.of(new TypeHint<String>() {
        })));
    }

    @Override
    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = source2Stat.value();
        if(value1 == null){
            //将soure1的数据存放到source1Stat中
            System.out.println("未接受到source2的数据，将source1的数据存放到状态中:" + value);
            source1Stat.update(value.f1);
            //如果10s内没有接受到数据，就测流输出
            long timeTs = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(timeTs);
            System.out.println("等待时间超过10s");
            timerStat.update(timeTs);
            currentKeyStat.update(value.f0);
        }else{
            //接受source2的数据，进行数据的相加
            value.f1 += value1;
            System.out.println("接受到source2的数据，将source1,source2的数据相加：" + value);
            out.collect(new Tuple2<>(value.f0,value.f1));
            ctx.timerService().deleteProcessingTimeTimer(timerStat.value());
            source1Stat.clear();
            source2Stat.clear();
            timerStat.clear();
            currentKeyStat.clear();
        }
    }

    @Override
    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = source1Stat.value();
        if(value1==null){
            System.out.println("未接受到source1的数据，将source2的数据存放到状态中:" + value);
            source2Stat.update(value.f1);
            //如果10s内没有接受到数据，就测流输出
            long timeTs = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(timeTs);
            System.out.println("等待时间超过10s");
            timerStat.update(timeTs);
            currentKeyStat.update(value.f0);
        }else{
            value.f1+=value1;
            System.out.println("接受到source1的数据，将source1,source2的数据相加：" + value);
            out.collect(new Tuple2<>(value.f0,value.f1));
            ctx.timerService().deleteProcessingTimeTimer(timerStat.value());
            source2Stat.clear();
            source1Stat.clear();
            timerStat.clear();
            currentKeyStat.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = source1Stat.value();
        Integer value2 = source2Stat.value();
        if(value1 != null){
            ctx.output(outputTag1,"传感器：延迟数据 key:" + currentKeyStat.value() + ",value :" + value1);
            source1Stat.clear();
            timerStat.clear();
            currentKeyStat.clear();
        }
        if(value2 != null){
            ctx.output(outputTag2,"传感器：延迟数据 key:" + currentKeyStat.value() + ",value :" + value2);
            source2Stat.clear();
            timerStat.clear();
            currentKeyStat.clear();
        }


    }

    @Override
    public void close() throws Exception {
        source1Stat.clear();
        source2Stat.clear();
        timerStat.clear();
        currentKeyStat.clear();
    }
}