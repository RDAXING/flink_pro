import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import scala.Int;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FlinkAPIOPeration {
    final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource1 = env.socketTextStream("localhost", 6666);
        DataStreamSource<String> dataSource2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> dataRes = dataSource1.connect(dataSource2).flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String s, Collector<String> collector) throws Exception {

                collector.collect("当前为类型String:" + s);

            }

            @Override
            public void flatMap2(String s, Collector<String> collector) throws Exception {
                collector.collect("当前为类型Integer:" + s);
            }
        });

        dataRes.print();
        env.execute();
    }

    private static String getDate(Long date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        return  sdf.format(new Date(date));

    }

    @Test
    public void reducedemo(){
        {
            DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
            dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    for (String ss : s.split(" ")) {
                        collector.collect(new Tuple2<>(ss, 1));
                    }
                }
            }).keyBy(f -> f.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {

                    return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                }
            }).keyBy(f -> f.f0).maxBy(1).print();



            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void windowOfTime(){
        {
            env.socketTextStream("localhost",6666).flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    for(String ss:s.split(" ")){
                        collector.collect(new Tuple2<>(ss,1));
                    }
                }
            }).keyBy(f->f.f0).timeWindow(Time.seconds(5))
                    .process(new ProcessWindowFunction<Tuple2<String,Integer>, Tuple3<String,String,Integer>, String, TimeWindow>() {
                        private transient  ValueState<Integer> sum ;
                        @Override
                        public void open(Configuration parameters) throws Exception {
                            sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", TypeInformation.of(new TypeHint<Integer>() {
                            })));
                        }

                        @Override
                        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                            Integer value = sum.value();
                            if(value == null){
                                value = 0;
                            }
                            for(Tuple2<String,Integer> t:iterable){
                                t.f1 += value;
                                sum.update(t.f1);
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                collector.collect(new Tuple3<>(s,getDate(start)+"_"+getDate(end),t.f1));
                            }
                        }

                        @Override
                        public void close() throws Exception {
                            sum.clear();
                        }
                    }).print();
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
