package processoption;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * 实时获取source数据处理10秒内temperature连续上升，
 * 则输出报警信息
 *
 */
public class KeyedProcessFunction_API2 {
    private static Logger LOG = LoggerFactory.getLogger(KeyedProcessFunction_API2.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        dataSource.map(new MapFunction<String, Tuple3<String,Long,Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                if(null ==s && s.split(",").length!=3){
                    return null;
                }
               String[] data = s.split(",");
                return new Tuple3<>(data[0], Long.parseLong(data[1]),Double.parseDouble(data[2]));
            }
        }).keyBy(f ->f.f0).process(new MyProcess(10)).print();

        try {
            env.execute("tempApp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置两个状态
     * tempStat用于存储温度
     * timerStat用于存储时间
     * 如果连续10s内temp处于上升，并且没有定时器时，则添加定时器
     * 否则清除定时器
     */
    private static class MyProcess extends  KeyedProcessFunction<String, Tuple3<String,Long,Double>, String>{
        private transient ValueState<Double> tempStat;
        private transient ValueState<Long> timerStat;
        private Integer interval;

        public MyProcess(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tempStat = getRuntimeContext().getState(new ValueStateDescriptor<Double>("tempStat", TypeInformation.of(new TypeHint<Double>() {
            })));
            timerStat = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerStat",TypeInformation.of(new TypeHint<Long>() {
            })));
        }

        @Override
        public void processElement(Tuple3<String, Long, Double> value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = tempStat.value();
            Long timerTs = timerStat.value();
            if(lastTemp == null){
                lastTemp = value.f2;
            }

            if(value.f2>lastTemp && timerTs == null){
                long timeStamp = ctx.timerService().currentProcessingTime() + interval * 1000;
                ctx.timerService().registerProcessingTimeTimer(timeStamp);
                timerStat.update(timeStamp);
            }

            if(value.f2 < lastTemp && timerTs != null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerStat.clear();
            }
            tempStat.update(value.f2);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器："+ctx.getCurrentKey() + " ,上升温度为：" + tempStat.value());
            timerStat.clear();
        }

        @Override
        public void close() throws Exception {
            tempStat.clear();
        }
    }
}
