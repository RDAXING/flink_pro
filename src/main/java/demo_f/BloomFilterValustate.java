package demo_f;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.curator4.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BloomFilterValustate {
    public static void main(String[] args) {
        //用户ID,  活动ID,  时间,                 事件类型,     省份
        //u001,    A1,      2019-09-02 10:10:11,  1,            北京市
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> dataSource = source.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple5<>(split[0], split[1], split[2], split[3], split[4]);
            }
        });

        dataSource.keyBy(new KeySelector<Tuple5<String,String,String,String,String>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple5<String, String, String, String, String> t5) throws Exception {
                return new Tuple2<>(t5.f1, t5.f3);
            }
        }).process(new KeyedProcessFunction<Tuple2<String,String>, Tuple5<String,String,String,String,String>, Tuple4<String,String,Integer,Integer>>() {
            private transient ValueState<BloomFilter> bloomFilterValueStat;
            private transient ValueState<Integer> uidCountStat;
            private transient ValueState<Integer> clickCountStat;
            @Override
            public void open(Configuration parameters) throws Exception {
                bloomFilterValueStat = getRuntimeContext().getState(new ValueStateDescriptor<BloomFilter>("bloomFilterStat", TypeInformation.of(new TypeHint<BloomFilter>() {
                })));
                uidCountStat = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("uidCountStat",TypeInformation.of(new TypeHint<Integer>() {
                })));
                clickCountStat = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("clickCountStat",TypeInformation.of(new TypeHint<Integer>() {
                })));
            }


            @Override
            public void processElement(Tuple5<String, String, String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                String uid = value.f0;
                String actId = value.f1;
                String type = value.f3;
                BloomFilter bloomV = bloomFilterValueStat.value();
                Integer uidCount = uidCountStat.value();
                Integer clickCount = clickCountStat.value();
                if(clickCount == null){
                    clickCount = 0;
                }
                if(bloomV == null){
                    bloomV =  BloomFilter.create(Funnels.unencodedCharsFunnel(),10000000);
                    uidCount=0;
                }

                if(!bloomV.mightContain(uid)){
                    bloomV.put(uid);
                    uidCount+=1;
                }
                clickCount+=1;

                bloomFilterValueStat.update(bloomV);
                uidCountStat.update(uidCount);
                clickCountStat.update(clickCount);
                out.collect(Tuple4.of(actId,type,uidCount,clickCount));
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
