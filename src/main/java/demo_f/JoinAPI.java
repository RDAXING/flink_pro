package demo_f;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;

public class JoinAPI {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        WatermarkStrategy<String> stringWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(500));
        DataStreamSource<Tuple2<String, Integer>> source1 = getSource(env, "source1");
        DataStreamSource<Tuple2<String, Integer>> source2 = getSource(env, "source2");
        source1.join(source2)
                .where(f->f.f0)
                .equalTo(f->f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {

                        return "商品："+t1.f0+" ，订单号：" + t1.f1+"_"+t2.f1;
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static DataStreamSource<Tuple2<String, Integer>> getSource(StreamExecutionEnvironment env,String name){
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private Boolean isRunning = true;
            private String[] type = {"sp", "sc", "hi", "hb"};

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning) {
                    Thread.sleep(1000);
                    Tuple2<String, Integer> tuple2 = new Tuple2<>(type[new Random().nextInt(type.length)], new Random().nextInt(100) + 100);
                    ctx.collect(tuple2);
                    System.out.println(name+":"+tuple2);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, name);
        return source;
    }

    private static DataStream<Tuple2<String, Integer>> getSocketSource(String ip,int port,StreamExecutionEnvironment env){
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream(ip, port);
        DataStream<Tuple2<String, Integer>> source = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
            }
        });
        return source;
    }

    @Test
    public void test(){
         String[] type = {"sp","sc","hi","hb"};
        String s = type[new Random().nextInt(type.length)];
        System.out.println(s);
    }
}
