package processoption.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyProcessWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<String> resultSource = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String ss : split) {
                    collector.collect(ss);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).keyBy(kv -> kv._1).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {

                    private ValueState<MyState> mystate;

                    //定义一个状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //初始化状态
                        /*
                        *
                            ValueState<T> getState(ValueStateDescriptor<T>)
                            ReducingState<T> getReducingState(ReducingStateDescriptor<T>)
                            ListState<T> getListState(ListStateDescriptor<T>)
                            AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT>)
                            MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)
                            */
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
                        for (Tuple2<String, Integer> value : iterable) {
                            count += value._2;
                        }

                        current.count += count;
                        mystate.update(current);
                        String record = MessageFormat.format("window: key: {0}, currenStartTime: {1} ,currentEndTime: {2} ,currentNum: {3} ,totalValue: {4}个",
                                current.key,
                                time(context.window().getStart()),
                                time(context.window().getEnd()),
                                count,
                                current.count
                        );

                        collector.collect(record);
                    }


                });

        resultSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String time(long timeStamp) {
        return new SimpleDateFormat("hh:mm:ss").format(new Date(timeStamp));
    }
}

class MyState{
    public String key;
    public Long count;
}


