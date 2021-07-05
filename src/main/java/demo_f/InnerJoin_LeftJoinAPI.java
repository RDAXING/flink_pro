package demo_f;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class InnerJoin_LeftJoinAPI {
    public static void main(String[] args) {
        int windowSize = 10;
        long delay = 5002L;
        //获取数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String path1 = "C:\\Users\\admin\\Desktop\\sparktest\\user\\join\\A.txt";
        String path2 = "C:\\Users\\admin\\Desktop\\sparktest\\user\\join\\B.txt";
        DataStreamSource<Tuple3<String, String, Long>> dataSource1 = env.addSource(new MydataSource(path1), "dataSource1");
        DataStreamSource<Tuple3<String, String, Long>> dataSource2 = env.addSource(new MydataSource(path2), "dataSource2");

        //设置watermark
        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStream  = dataSource1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Long>> rigtStream  = dataSource2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });

        leftStream.join(rigtStream)
                .where(f->f.f0)
                .equalTo(f->f.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new JoinFunction<Tuple3<String,String,Long>, Tuple3<String,String,Long>, Tuple5<String,String,String,Long,Long>>() {
                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> t1, Tuple3<String, String, Long> t2) throws Exception {


                        return new Tuple5<>(t1.f0,t1.f1,t2.f1,t1.f2,t2.f2);
                    }
                }).print("inner join");


        //左连接
        leftStream.coGroup(rigtStream).where(f->f.f0)
                .equalTo(f->f.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new CoGroupFunction<Tuple3<String,String,Long>, Tuple3<String,String,Long>, Tuple5<String,String,String,Long,Long>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> iterable, Iterable<Tuple3<String, String, Long>> iterable1, Collector<Tuple5<String, String, String, Long, Long>> collector) throws Exception {
                        HashMap<String, Tuple3<String, String, Long>> t2Map = new HashMap<>();
                        iterable1.forEach(a ->{
                            t2Map.put(a.f0,a);
                        });

                        for(Tuple3<String,String,Long> t1s:iterable){
                            if(t2Map.containsKey(t1s.f0)){
                                Tuple3<String, String, Long> t2value = t2Map.get(t1s.f0);
                                collector.collect(new Tuple5<>(t1s.f0,t1s.f1,t2value.f1,t1s.f2,t2value.f2));
                            }else{
                                collector.collect(new Tuple5<>(t1s.f0,t1s.f1,null,t1s.f2,-1l));
                            }

                        }

                    }
                }).print("left join");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

class MydataSource extends RichParallelSourceFunction<Tuple3<String,String,Long>>{

    private boolean flag = true;
    private String fileName;

    public MydataSource(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        List<String> list = FileUtils.readLines(new File(fileName), "UTF-8");
        Iterator<String> it = list.iterator();
        while(flag && it.hasNext()){
            String value = it.next();
            String[] datas = value.split(",");
            ctx.collect(new Tuple3<>(datas[0],datas[1],Long.parseLong(datas[2])));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}