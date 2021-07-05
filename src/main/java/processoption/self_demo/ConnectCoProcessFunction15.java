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
public class ConnectCoProcessFunction15 {
    public static void main(String[] args) {
        OutputTag<Tuple2<String,Integer>> tag1 = new OutputTag<Tuple2<String, Integer>>("Tag1"){};
        OutputTag<Tuple2<String,Integer>> tag2 = new OutputTag<Tuple2<String, Integer>>("Tag2"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<String, Integer>, String> source1 = getSocketSource("localhost", 6666, env);
        KeyedStream<Tuple2<String, Integer>, String> source2 = getSocketSource("localhost", 7777, env);
        //两个流进行connect
        SingleOutputStreamOperator<Tuple2<String, Integer>> reultSource = source1.connect(source2).process(new MyCoProcess(tag1,tag2));
        reultSource.print("connect source");
        reultSource.getSideOutput(tag1).print("Tag1 stat1");
        reultSource.getSideOutput(tag2).print("Tag2 stat2");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private static KeyedStream<Tuple2<String, Integer>, String> getSocketSource(String ip, int port, StreamExecutionEnvironment env){
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream(ip, port);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {

                if (StringUtils.isBlank(s) && s.split(",").length == 2) {
                    return null;
                }
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Integer.parseInt(split[1]));
            }
        }).keyBy(f -> f.f0);

        return keyedStream;
    }
}

class MyCoProcess extends CoProcessFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>>{
    private static final Integer interval = 10000;
    private transient ValueState<Integer> source1Stat;
    private transient ValueState<Integer> source2Stat;
    private transient ValueState<Long> timerStat;
    private transient ValueState<String> currentKeyStat;
    private OutputTag<Tuple2<String,Integer>> tag1;
    private OutputTag<Tuple2<String,Integer>> tag2;

    public MyCoProcess(OutputTag<Tuple2<String, Integer>> tag1, OutputTag<Tuple2<String, Integer>> tag2) {
        this.tag1 = tag1;
        this.tag2 = tag2;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        source1Stat = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("source1Stat", TypeInformation.of(new TypeHint<Integer>() {
        })));
        source2Stat = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("source2Stat", TypeInformation.of(new TypeHint<Integer>() {
        })));
        timerStat = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerStat", TypeInformation.of(new TypeHint<Long>() {
        })));
        currentKeyStat=getRuntimeContext().getState(new ValueStateDescriptor<String>("currentKeyStat",TypeInformation.of(new TypeHint<String>() {
        })));
    }

    @Override
    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value2 = source2Stat.value();
        Integer value1 = source1Stat.value();
        if(null == value1){
            value1 = 0;
        }
        if(null == value2){
            System.out.println("未接受到2号流数据 key:"+value.f0+"相应的值，将1号流数据存放到source1Stat中");
            source1Stat.update(value.f1+=value1);
            long onTimers = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(onTimers);
            System.out.println("如果时间超出10,创建定时器");
            timerStat.update(onTimers);
            currentKeyStat.update(value.f0);
        }else{
            System.out.println("接受到2号流数据，将1号流数据,2号流数据进行相加");
            value.f1+=value2;
            out.collect(new Tuple2<>(value.f0,value.f1));
            ctx.timerService().deleteProcessingTimeTimer(timerStat.value());
            source2Stat.clear();
            source1Stat.clear();
            timerStat.clear();
            currentKeyStat.clear();
        }
    }

    @Override
    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = source1Stat.value();
        Integer value2 = source2Stat.value();
        if(value2 == null){
            value2 = 0;
        }
        if(null == value1){
            System.out.println("未接受到1号流数 key:"+value.f0+"相应的值，将2号流数据存放到source2Stat中");
            source2Stat.update(value.f1+=value2);
            long onTimers = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(onTimers);
            System.out.println("如果时间超出10,创建定时器");
            timerStat.update(onTimers);
            currentKeyStat.update(value.f0);

        }else{
            System.out.println("接受到1号流数据，将2号流数据,1号流数据进行相加");
            value.f1+=value1;
            out.collect(new Tuple2<>(value.f0,value.f1));
            ctx.timerService().deleteProcessingTimeTimer(timerStat.value());
            source1Stat.clear();
            source2Stat.clear();
            timerStat.clear();
            currentKeyStat.clear();
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer stat1 = source1Stat.value();
        Integer stat2 = source2Stat.value();
        if(null != stat1){
            System.out.println("10s 内未接受到2号流数据，将1号数据测流输出");
            ctx.output(tag1,new Tuple2<>(currentKeyStat.value(),stat1));
        }
        if(null != stat2){
            System.out.println("10s 内未接受到1号流数据，将2号数据测流输出");
            ctx.output(tag2,new Tuple2<>(currentKeyStat.value(),stat2));
        }
    }

    @Override
    public void close() throws Exception {
        source2Stat.clear();
        source1Stat.clear();
        timerStat.clear();
        currentKeyStat.clear();
    }
}
