package state.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 想知道两次事件hainiu之间，一共发生多少次其他事件，分别是什么事件
 * 事件流：hai a a a a a f d d hai ad d s s d hainiu…
 * 当事件流中出现字母e时触发容错
 * 输出：
 * (8,a a a a a f d d)
 * (6,ad d s s d)
 */

/**
 * 测流输出sideOutPut+状态存储valueState
 * 程序说明：
 * 统计word数，然后通过测流输出基数或者偶数
 */
public class SideOutPutAPI {
    public static void main(String[] args) {
        //偶数测流标记
        OutputTag<Tuple2<String, Integer>> outputTag1 = new OutputTag<Tuple2<String, Integer>>("side-num1"){};
        //奇数测流标记
        OutputTag<Tuple2<String, Integer>> outputTag2 = new OutputTag<Tuple2<String, Integer>>("side-num2"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //通过socket流模仿kafka进行数据源的输入
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> process = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(new Tuple2<>(ss, 1));
                }
            }
        }).keyBy(0).flatMap(new WordCountFlatMapFunction()).process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> t1, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Integer f1 = t1.f1;
                if (f1 % 2 == 0) {
                    context.output(outputTag1, new Tuple2<>(t1.f0, t1.f1));
                } else {
                    context.output(outputTag2, new Tuple2<>(t1.f0, t1.f1));
                }

                collector.collect(new Tuple2<>(t1.f0, t1.f1));
            }
        });

        process.getSideOutput(outputTag1).print();//打印统计结果为偶数的值
//        process.getSideOutput(outputTag2).print();//打印统计结果为基数的值

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 通过状态存储中间值sum
     */
    private static class WordCountFlatMapFunction extends RichFlatMapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>{

        private transient  ValueState<Integer> sum;
        @Override
        public void open(Configuration parameters) throws Exception {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
        }

        @Override
        public void flatMap(Tuple2<String, Integer> t, Collector<Tuple2<String, Integer>> collector) throws Exception {
            Integer value = sum.value();
            if(value == null ){
                value = 0;
            }

            sum.update(t.f1+=value);

            collector.collect(new Tuple2<>(t.f0,t.f1));
        }

        @Override
        public void close() throws Exception {
            sum.clear();
        }
    }
}
