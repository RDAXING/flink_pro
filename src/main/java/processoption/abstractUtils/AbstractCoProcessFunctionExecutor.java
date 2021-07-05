package processoption.abstractUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.log4j.Logger;

/**
 * AbstractCoProcessFunctionExecutor做了很多事情，
 * 唯独没有实现双流连接后的具体业务逻辑，这些没有做的是留给子类来实现的，
 * 整个三部曲系列的重点都集中在AbstractCoProcessFunctionExecutor的子类上，
 * 把双流连接后的业务逻辑做好
 */
public abstract class AbstractCoProcessFunctionExecutor {
    private static Logger logger = Logger.getLogger(AbstractCoProcessFunctionExecutor.class);
    /**
     * 子类自定义实现抽象方法
     * 主要用于对双流进行处理
     * @return
     */
    protected abstract CoProcessFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>> getCoProcessFunctionInstance();

    /**
     * 获取socket流数据
     * @param ip
     * @param port
     * @param env
     * @return
     */
    protected KeyedStream<Tuple2<String, Integer>, String>  buildStreamFromSocket(String ip, int port, StreamExecutionEnvironment env){
        DataStreamSource<String> dataSource = env.socketTextStream(ip, port);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = dataSource.map(new MyWordCount()).keyBy(f -> f.f0);
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

    /**
     * 如果子类有侧输出需要处理，请重写此方法，会在主流程执行完毕后被调用
     */
    protected void doSideOutput(SingleOutputStreamOperator<Tuple2<String, Integer>> mainDataStream) {
    }

 protected void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<Tuple2<String, Integer>, String> dataSource1 = buildStreamFromSocket("localhost", 6666, env);
        KeyedStream<Tuple2<String, Integer>, String> dataSource2 = buildStreamFromSocket("localhost", 7777, env);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resSource =
                dataSource1.connect(dataSource2).process(getCoProcessFunctionInstance());
        resSource.print();
        doSideOutput(resSource);
        env.execute();
    }
}
