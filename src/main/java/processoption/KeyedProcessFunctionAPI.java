package processoption;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        dataSource.map(new MapFunction<String, Tuple3<String,Long,Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] split = s.split(",");
                if(null == split || split.length != 3){
                    return new Tuple3<>();
                }
                return new Tuple3<>(split[0],Long.parseLong(split[1]),Double.parseDouble(split[2]));
            }
        }).keyBy(f->f.f0).process(new MyProcess()).print();
        env.execute();
    }

    private static class MyProcess extends  KeyedProcessFunction<String, Tuple3<String,Long,Double>, String>{

        @Override
        public void processElement(Tuple3<String, Long, Double> t1, Context context, Collector<String> collector) throws Exception {
            collector.collect(context.getCurrentKey()+",温度："+ t1.f2);
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+2000);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("触发器触发时间：" + timestamp);

        }
    }
}
