package processoption;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import processoption.abstractUtils.AbstractCoProcessFunctionExecutor;

public class CoProcessFunctionAPI extends AbstractCoProcessFunctionExecutor {
    @Override
    protected CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> getCoProcessFunctionInstance() {
        return new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>("socket one:"+value.f0,value.f1));
            }

            @Override
            public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>("socket two:"+value.f0,value.f1));
            }
        };
    }

    public static void main(String[] args) {
        try {
            new CoProcessFunctionAPI().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
