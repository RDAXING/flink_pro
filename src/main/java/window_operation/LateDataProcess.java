package window_operation;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.text.ParseException;
import java.time.Duration;
import java.util.List;

public class LateDataProcess {
    /**
     * 延时数据的处理
     * @param args
     */
    public static void main(String[] args) {
        //获取数据流
        OutputTag<Tuple3<String, Long, Double>> outputTag = new OutputTag<Tuple3<String, Long, Double>>("lateData") {};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Double>> aggSource = dataSource.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple3<>(ss[0], getDateToTime(ss[1]), Double.parseDouble(ss[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Double>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> t3, long l) {
                return t3.f1;
            }
        })).keyBy(f -> f.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                .aggregate(new AggregateFunction<Tuple3<String, Long, Double>, Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> createAccumulator() {
                        return new Tuple2<>("", 0d);
                    }

                    @Override
                    public Tuple2<String, Double> add(Tuple3<String, Long, Double> t1, Tuple2<String, Double> t2) {
                        t2.f1 += t1.f2;
                        return new Tuple2<>(t1.f0, t2.f1);
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple2<String, Double> stringDoubleTuple2) {
                        return stringDoubleTuple2;
                    }

                    @Override
                    public Tuple2<String, Double> merge(Tuple2<String, Double> t2, Tuple2<String, Double> acc1) {
                        return new Tuple2<>(t2.f0, t2.f1 += acc1.f1);
                    }
                });

        aggSource.print("正常数据");
        aggSource.getSideOutput(outputTag).print("迟到数据");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Long getDateToTime(String date){
        FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        long time = Long.MIN_VALUE;
        try {
            time = fdf.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }


    /**
     * 通过process进行测流的输出
     */
    @Test
    public void getLateData2(){
        OutputTag<Tuple3<String,Long,Double>> outputTag = new OutputTag<Tuple3<String, Long, Double>>("lateData"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Double>> processSource = dataSource.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple3<>(ss[0], getDateToTime(ss[1]), Double.parseDouble(ss[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Double>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> t3, long l) {
                return t3.f1;
            }
        })).process(new MyProcessFunction(outputTag));


        processSource.getSideOutput(outputTag).print("迟到数据");
        processSource.print("正常数据");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class MyProcessFunction extends ProcessFunction<Tuple3<String,Long,Double>, Tuple2<String,Double>>{

        private OutputTag<Tuple3<String, Long, Double>> outputTag;
        public MyProcessFunction(OutputTag<Tuple3<String, Long, Double>> outputTag) {
            this.outputTag = outputTag;
        }



        @Override
        public void processElement(Tuple3<String, Long, Double> value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {

            if(value.f1 < ctx.timerService().currentWatermark()){
                ctx.output(outputTag,value);
            }else{
                out.collect(new Tuple2<>(value.f0,value.f2));
            }
        }

    }


    /**
     * 延迟数据的处理
     * 1.waterMaker设置
     * 2.sideoutput
     * 3.allowLatness
     */
    @Test
    public void LateDataProcess(){
        OutputTag<Tuple3<String, Long, Double>> outputTag = new OutputTag<Tuple3<String, Long, Double>>("lateDataTag") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Double>> processStream = dataSource.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple3<>(ss[0], getDateToTime(ss[1]), Double.parseDouble(ss[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Double>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> t3, long l) {
                return t3.f1;
            }
        })).keyBy(f -> f.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .allowedLateness(Time.milliseconds(1000))
                .process(new MyProcessWindowFunction(outputTag));


        processStream.print("正常数据");
        processStream.getSideOutput(outputTag).print("延迟数据");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String,Long,Double>, Tuple2<String,Double>, String, TimeWindow>{

        private transient ValueState<Double> sumState ;
        private OutputTag<Tuple3<String,Long,Double>> outputTag ;

        public MyProcessWindowFunction(OutputTag<Tuple3<String, Long, Double>> outputTag) {
            this.outputTag = outputTag;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            sumState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("sumstate",TypeInformation.of(new TypeHint<Double>() {
            })));
        }

        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Long, Double>> elements, Collector<Tuple2<String, Double>> out) throws Exception {
            Double sumNum = sumState.value();


            for(Tuple3<String, Long, Double> num : elements){
                System.out.println("数据时间：" + num.f1 +"当前水位线："+context.currentWatermark());
                if(num.f1 < context.currentWatermark()){
                    context.output(outputTag,num);
                } else {
                    if(sumNum == null){
                        sumNum = num.f2;
                    }else{

                        sumNum += num.f2;
                        out.collect(new Tuple2<>(s,sumNum));
                        sumState.update(sumNum);
                    }
                }
            }


        }

        @Override
        public void close() throws Exception {
            sumState.clear();
        }
    }



}
