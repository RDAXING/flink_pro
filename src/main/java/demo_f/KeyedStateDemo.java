package demo_f;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KeyedStateDemo {
    /**
     * listState的使用
     */
    @Test
    public void ListStateD(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, List<String>>> listStateSource = dataSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                if (split != null && split.length == 2) {

                    return Tuple2.of(split[0], split[1]);
                }
                return null;
            }
        }).keyBy(f -> f.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, List<String>>, String, TimeWindow>() {
                    ListState<String> listState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", String.class));

                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, List<String>>> collector) throws Exception {
                        for (Tuple2<String, String> t : iterable) {
                            listState.add(t.f1);
                        }

                        Iterable<String> newState = listState.get();
                        ArrayList<String> list = new ArrayList<>();
                        for (String ss : newState) {
                            list.add(ss);
                        }

                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String keys = MessageFormat.format("key:{0},time:{1}", s, start + "--" + end);
                        collector.collect(Tuple2.of(keys, list));

                        listState.update(list);
                    }
                });
        listStateSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * valueState实现listState
     */
    @Test
    public void valueStateD(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, List<String>>> valueStateSource = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    String[] split = ss.split(",");
                    collector.collect(Tuple2.of(split[0], split[1]));
                }
            }
        }).keyBy(f -> f.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, List<String>>, String, TimeWindow>() {
                    private transient ValueState<List<String>> valueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<List<String>>("valueState", TypeInformation.of(new TypeHint<List<String>>() {
                        })));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, List<String>>> collector) throws Exception {
                        List<String> value = valueState.value();
                        if (value == null) {
                            value = new ArrayList<String>();
                        }
                        for (Tuple2<String, String> va : iterable) {
                            value.add(va.f1);
                        }

                        valueState.update(value);
                        collector.collect(Tuple2.of(s, valueState.value()));
                    }
                });

        valueStateSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * MapState的使用
     */
    @Test
    public void MapStateD(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> resSource = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Double>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Double>> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    String[] split = ss.split(",");
                    collector.collect(Tuple2.of(split[0], Double.parseDouble(split[1])));
                }
            }
        }).keyBy(f -> f.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MyMapState(5.0));

        resSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyMapState extends ProcessWindowFunction<Tuple2<String,Double>, Tuple3<String,Integer,Integer>, String, TimeWindow>{
        private Double diffSize ;
        private transient  MapState<String, Integer> mapState;
        public MyMapState(Double diffSize) {
            this.diffSize = diffSize;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("mapState", String.class, Integer.class));

        }

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Double>> iterable, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
            boolean empty = mapState.isEmpty();
            if(empty){
                if(!mapState.contains("up")){
                    mapState.put("up",0);
                }
                if(!mapState.contains("down")){
                    mapState.put("down",0);
                }
            }else{

                for(Tuple2<String, Double> t:iterable){
                    Double currentD = t.f1;
                    if(currentD>diffSize){
                        mapState.put("up",mapState.get("up") + 1);
                    }else{
                        mapState.put("down",mapState.get("down") + 1);
                    }
                }
                collector.collect(Tuple3.of(s+",time:"+context.window().getStart()+"--"+context.window().getEnd()  ,mapState.get("up"),mapState.get("down")));
            }

        }
    }
}
