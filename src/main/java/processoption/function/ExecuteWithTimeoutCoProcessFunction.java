package processoption.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 通过connect连接两个流，进行两个流相同key进行value的相加，如果超过10秒中只要其中一个没有接受到数据，就测流输出
 */
public class ExecuteWithTimeoutCoProcessFunction extends CoProcessFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteWithTimeoutCoProcessFunction.class);

    private static final Long waitTime = 10000L;
    private OutputTag<String> sourceflow1;
    private OutputTag<String> sourceflow2;

    private static ValueState<Integer> statNum1;
    private static ValueState<Integer> statNum2;
    private static ValueState<Long> statTimer;
    private static ValueState<String> statCurrentKey;

    public ExecuteWithTimeoutCoProcessFunction(OutputTag<String> sourceflow1, OutputTag<String> sourceflow2) {
        this.sourceflow1 = sourceflow1;
        this.sourceflow2 = sourceflow2;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        statNum1 = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("statNum1", Integer.class));
        statNum2 = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("statNum2", Integer.class));
        statTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("statTimer", Long.class));
        statCurrentKey = getRuntimeContext().getState(new ValueStateDescriptor<String>("statCurrentKey", String.class));
    }

    @Override
    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

        Integer value2 = statNum2.value();
        if(null  == value2){
            logger.info("processElement1：2号流还未收到过[{}]，把1号流收到的值[{}]保存起来", value.f0, value.f1);
            statNum1.update(value.f1);
            statCurrentKey.update(value.f0);
            long timer = ctx.timerService().currentProcessingTime() + waitTime;
            ctx.timerService().registerProcessingTimeTimer(timer);
            statTimer.update(timer);
            logger.info("processElement1：创建定时器[{}]，等待2号流接收数据", new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date(timer)));
        }else{
            out.collect(new Tuple2<>(value.f0,value.f1+=value2));
            //取消触发定时器
            Long currentTimer = statTimer.value();
            ctx.timerService().deleteProcessingTimeTimer(currentTimer);
            logger.info("processElement1：取消定时器[{}]", new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date(currentTimer)));
            closeAllStat();
        }
    }

    @Override
    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer value1 = statNum1.value();
        if(null  == value1){
            logger.info("processElement2：1号流还未收到过[{}]，把2号流收到的值[{}]保存起来", value.f0, value.f1);
            statNum2.update(value.f1);
            statCurrentKey.update(value.f0);
            long timer = ctx.timerService().currentProcessingTime() + waitTime;
            ctx.timerService().registerProcessingTimeTimer(timer);
            statTimer.update(timer);
            logger.info("processElement2：创建定时器[{}]，等待1号流接收数据", new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date(timer)));
        }else{
            out.collect(new Tuple2<>(value.f0,value.f1+=value1));
            //取消触发定时器
            Long currentTimer = statTimer.value();
            ctx.timerService().deleteProcessingTimeTimer(currentTimer);
            logger.info("processElement1：取消定时器[{}]", new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date(currentTimer)));
            closeAllStat();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        String currentkey = statCurrentKey.value();
        Integer value1 = statNum1.value();
        Integer value2 = statNum2.value();
        if(value1 != null){
            ctx.output(sourceflow1,"时间超过10秒---》key:"+currentkey+";value:" + value1);
        }
        if(value2 != null){
            ctx.output(sourceflow2,"时间超过10秒---》key:"+currentkey+";value:" + value2);

        }
        closeAllStat();
    }


    public static void closeAllStat(){
        statNum1.clear();
        statNum2.clear();
        statCurrentKey.clear();
        statTimer.clear();
    }

//    @Override
//    public void close() throws Exception {
//        closeAllStat();
//    }
}
