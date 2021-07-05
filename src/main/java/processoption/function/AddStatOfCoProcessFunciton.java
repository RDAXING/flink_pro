package processoption.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import processoption.abstractUtils.AbstractCoProcessFunctionExecutor;

public class AddStatOfCoProcessFunciton extends AbstractCoProcessFunctionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(AddStatOfCoProcessFunciton.class);
    @Override
    protected CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> getCoProcessFunctionInstance() {
        return new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private  transient ValueState<Integer> source_1;
            private  transient ValueState<Integer> source_2;
            @Override
            public void open(Configuration parameters) throws Exception {
                source_1 = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("source_1", TypeInformation.of(new TypeHint<Integer>() {
                })));
                source_2 = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("source_2", TypeInformation.of(new TypeHint<Integer>() {
                })));
            }

            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                logger.info("处理元素1：{}", value);
                Integer value2 = source_2.value();
                if(value2 == null){
                    logger.info("2号流还未收到过[{}]，把1号流收到的值[{}]保存起来", value.f0, value.f1);
                    source_1.update(value.f1);
                }else{
                    logger.info("2号流收到过[{}]，值是[{}]，现在把两个值相加后输出", value.f0, value2);
                    out.collect(new Tuple2<>(value.f0,value.f1+=value2));
//                    source_1.update(value.f1);
                }
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                logger.info("处理元素2：{}", value);
                Integer value1 = source_1.value();
                if(value1 == null){
                    logger.info("1号流还未收到过[{}]，把1号流收到的值[{}]保存起来", value.f0, value.f1);
                    source_2.update(value.f1);
                }else{
                    logger.info("1号流收到过[{}]，值是[{}]，现在把两个值相加后输出", value.f0, value1);
                    out.collect(new Tuple2<>(value.f0,value.f1+=value1));
//                    source_2.update(value.f1);
                }
            }

            @Override
            public void close() throws Exception {
                source_1.clear();
                source_2.clear();
            }
        };
    }

    public static void main(String[] args) {
        try {
            new AddStatOfCoProcessFunciton().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
