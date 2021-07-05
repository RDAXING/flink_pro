package demo_f;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WindowAllOfWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("", 6666);
        SingleOutputStreamOperator<String> resultSource = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(Tuple2.of(ss, 1));
                }
            }
        }).keyBy(k -> k.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    ValueState<MyState> mystate = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        mystate = getRuntimeContext().getState(new ValueStateDescriptor<>("mystate", MyState.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
                        MyState current = mystate.value();
                        if (current == null) {
                            current = new MyState();
                            current.key = s;
                            current.count = 0l;
                        }

                        Integer count = 0;
                        for (Tuple2<String, Integer> t : iterable) {
                            count += t.f1;
                        }
                        current.count += count;
                        mystate.update(current);
                        String format = MessageFormat.format("key:{0},startTime:{1},endTime:{2},count:{3}",
                                s, time(context.window().getStart()), time(context.window().getEnd()), current.count
                        );
                        collector.collect(format);
                    }
                });

        resultSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static String time(Long timestamp){
        return  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }
}



class MyState{
    String key;
    Long count;
}