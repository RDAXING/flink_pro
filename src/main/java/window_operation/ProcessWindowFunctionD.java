package window_operation;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class ProcessWindowFunctionD {
    public static void main(String[] args) {
         //获取socket流的数据然后统计每5s窗口的数量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2Source = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(new Tuple2<>(ss, 1));
                }
            }
        });

        tuple2Source.keyBy(f->f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new MyProcessWindowFunction()).print("count-export");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Integer>, Tuple3<String,String,Integer>, String, TimeWindow> {
        private transient ValueState<Integer> countState;
        private Integer currntCount ;
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("countState", TypeInformation.of(new TypeHint<Integer>() {
            })));
        }

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            Integer value = countState.value();
            if(value == null ){
                value = 0;
            }

            for(Tuple2<String,Integer> t2  : elements){
                value += t2.f1;
                long start = context.window().getStart();
                long end = context.window().getEnd();

                out.collect(new Tuple3<>(s,getDate(start,end),value));
                countState.update(value);
            }


        }

        @Override
        public void close() throws Exception {
            countState.clear();
        }
    }

    public static  String getDate(Long start,Long end){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startS = sdf.format(new Date(start));
        String endS = sdf.format(new Date(end));
        return "开始时间：" + startS + "；结束时间："+ endS;
    }
}
