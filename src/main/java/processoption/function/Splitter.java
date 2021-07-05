package processoption.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        if(StringUtils.isBlank(s)){
            System.out.println("invalide line");
            return;
        }else{
            for(String value : s.split(" ")){
                collector.collect(Tuple2.of(value,1));
            }
        }
    }
}
