package demo_f;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class WindowOption {
    /**
     * aggregate案例的使用
     */
    @Test
    public void aggregatedemo(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> source1 = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(new Tuple2<>(ss, 1));
                }
            }
        });

        SingleOutputStreamOperator<String> sumRes = source1.keyBy(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //AggregateFunction<IN, ACC, OUT> extends Function, Serializable
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple3<String, Long, Integer>, String>() {
                    @Override
                    public Tuple3<String, Long, Integer> createAccumulator() {
                        return Tuple3.of("", 0L, 0);
                    }

                    @Override
                    public Tuple3<String, Long, Integer> add(Tuple2<String, Integer> t1, Tuple3<String, Long, Integer> t2) {
                        return Tuple3.of(t1.f0, System.currentTimeMillis(), t1.f1 + t2.f2);
                    }

                    @Override
                    public String getResult(Tuple3<String, Long, Integer> t) {
                        return t.f0 + "," + t.f1 + "," + t.f2;
                    }

                    @Override
                    public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> t1, Tuple3<String, Long, Integer> acc1) {
                        return Tuple3.of(acc1.f0, t1.f1 > acc1.f1 ? t1.f1 : acc1.f1, acc1.f2 + t1.f2);
                    }
                });

        sumRes.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 全窗口函数之apply
     * public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {
     void apply(KEY var1, W var2, Iterable<IN> var3, Collector<OUT> var4) throws Exception;
     }
     *
     */
    @Test
    public void fullWindowDemo(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resultSource = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(new Tuple2<>(ss, 1));
                }
            }
        }).keyBy(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                        Integer sum = 0;
                        for (Tuple2<String, Integer> t : iterable) {
                            sum += t.f1;
                        }
                        collector.collect(Tuple3.of(s, timeWindow.getStart() + "--" + timeWindow.getEnd(), sum));
                    }
                });

        resultSource.print("apply");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *计数窗口的使用
     */
    @Test
    public void countWindow(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Double>> resSource = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Double>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Double>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    String[] split = ss.split(";");
                    collector.collect(Tuple2.of(split[0], Double.parseDouble(split[1])));
                }
            }
        }).keyBy(f -> f.f0).countWindow(10, 2)
                .aggregate(new AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple3<String, Double, Integer> createAccumulator() {
                        return Tuple3.of("", 0.0, 0);
                    }

                    @Override
                    public Tuple3<String, Double, Integer> add(Tuple2<String, Double> t1, Tuple3<String, Double, Integer> t2) {
                        return Tuple3.of(t1.f0, t1.f1 + t2.f1, t2.f2 + 1);
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> t) {
                        return Tuple2.of(t.f0, t.f1 / t.f2);
                    }

                    @Override
                    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> t1, Tuple3<String, Double, Integer> acc1) {
                        return Tuple3.of(t1.f0, t1.f1 + acc1.f1, t1.f2 + acc1.f2);
                    }
                });
        resSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
