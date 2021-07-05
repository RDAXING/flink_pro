package processoption;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;
import processoption.abstractUtils.AbstractCoProcessFunctionExecutor;
import processoption.function.ExecuteWithTimeoutCoProcessFunction;

public class AddTwoSourceValueWithTimeout{
    private static Logger logger = Logger.getLogger(AbstractCoProcessFunctionExecutor.class);
    public static void main(String[] args) {
        OutputTag<String> outputTag1 = new OutputTag<String>("tag1") {
        };
        OutputTag<String> outputTag2 = new OutputTag<String>("tag2") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple2<String, Integer>, String> source1 = getSocketData("localhost", 6666, env);
        KeyedStream<Tuple2<String, Integer>, String> source2 = getSocketData("localhost", 7777, env);
        SingleOutputStreamOperator<Tuple2<String, Integer>> processSource = source1.connect(source2).process(new ExecuteWithTimeoutCoProcessFunction(outputTag1, outputTag2));
        processSource.print();
        processSource.getSideOutput(outputTag1).print();
        processSource.getSideOutput(outputTag2).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static KeyedStream<Tuple2<String, Integer>, String> getSocketData(String ip,int port,StreamExecutionEnvironment env){
        env.setParallelism(1);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = env.socketTextStream(ip, port).map(new MyWordCount()).keyBy(f -> f.f0);
        return keyedStream;
    }

    private static class MyWordCount implements MapFunction<String,Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            if(StringUtils.isBlank(s)){
                logger.warn("current value is null");
                return null;
            }
            String[] split = s.split(",");
            if(split.length != 2){
                logger.warn("current value length is not 2");
                return null;
            }
            return new Tuple2<>(split[0],Integer.parseInt(split[1]));
        }
    }

}
