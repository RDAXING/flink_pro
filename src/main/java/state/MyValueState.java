package state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class MyValueState {
    /**
     * valuestate 未使用窗口函数进行wordcount求和
     * @param args
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFunction<Tuple2<String,Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
                sourceContext.collect(Tuple2.of("a",2));
                Thread.sleep(2000);
                sourceContext.collect(Tuple2.of("b",1));
                Thread.sleep(2000);
                sourceContext.collect(Tuple2.of("a",3));
                Thread.sleep(2000);
                sourceContext.collect(Tuple2.of("a",1));
                Thread.sleep(2000);
                sourceContext.collect(Tuple2.of("b",4));
                Thread.sleep(2000);
                sourceContext.collect(Tuple2.of("c",3));
                Thread.sleep(2000);
                sourceContext.collect(Tuple2.of("b",5));
                Thread.sleep(2000);
            }

            @Override
            public void cancel() {

            }
        }).keyBy(f -> f.f0)
                .flatMap(new CountWindowSum()).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class CountWindowSum extends RichFlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>{

        private transient  ValueState<Tuple2<String, Integer>> sumstate;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sumstate = getRuntimeContext().getState(new ValueStateDescriptor<>("sumstate", TypeInformation.of(
                    new TypeHint<Tuple2<String, Integer>>() {
                    }
            )));

        }

        @Override
        public void flatMap(Tuple2<String, Integer> input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            Tuple2<String, Integer> value = sumstate.value();
            if(null == value){
                value = Tuple2.of("",0);
            }
            value.f1 += input.f1;
            sumstate.update(Tuple2.of(input.f0,value.f1));
            collector.collect(Tuple2.of(input.f0,value.f1));
            sumstate.clear();
        }
    }
}
