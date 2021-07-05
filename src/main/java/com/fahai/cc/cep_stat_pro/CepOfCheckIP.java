package com.fahai.cc.cep_stat_pro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * flink复杂事件的处理CEP
 */
public class CepOfCheckIP {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\cep.txt";
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        KeyedStream<Tuple2<String, String>, String> keyedStream = dataSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[1], s);
            }
        }).keyBy(f -> f.f0);
        //定义Pattern,指定相关条件和模型序列
        Pattern<Tuple2<String, String>, Tuple2<String, String>> pa = Pattern.<Tuple2<String, String>>begin("start").where(new SimpleCondition<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> t1) throws Exception {

                return t1.f1 != null;
            }
        }).next("second").where(new IterativeCondition<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> t2, Context<Tuple2<String, String>> context) throws Exception {
                Iterator<Tuple2<String, String>> start = context.getEventsForPattern("start").iterator();
                while (start.hasNext()) {
                    Tuple2<String, String> lastValue = start.next();
                    if (!lastValue.f1.split(",")[0].equals(t2.f1.split(",")[0])) {
                        return true;
                    }
                }
                return false;
            }
        });

        PatternStream<Tuple2<String, String>> patternStream = CEP.pattern(keyedStream, pa);

        patternStream.select(new MyPatternSelectFunction()).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

class MyPatternSelectFunction implements   PatternSelectFunction<Tuple2<String,String>, String>{


    @Override
    public String select(Map<String, List<Tuple2<String, String>>> map) throws Exception {
        Iterator<Tuple2<String, String>> start = map.get("start").iterator();
        if(start.hasNext()){
            System.out.println("满足start模式中的数据:"+start.next());
        }

        Iterator<Tuple2<String, String>> second = map.get("second").iterator();
        String result = "";
        if(second.hasNext()){
            result = second.next().f1;
            System.out.println("满足second模式中的数据:" + result);
        }
        return result;
    }
}
