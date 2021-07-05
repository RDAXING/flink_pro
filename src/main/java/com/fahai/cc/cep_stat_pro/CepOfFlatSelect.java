package com.fahai.cc.cep_stat_pro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * author rdx
 * //mid对应设备，page_id对应当前访问页面，ts访问时间戳，last_page_id指来源页面（有该字段代表从其他页面跳转过来）
 {common:{mid:101},page:{page_id:home},ts:10000} ,
 {common:{mid:102},page:{page_id:home},ts:12000},
 {common:{mid:102},page:{page_id:good_list,last_page_id:home},ts:15000} ,
 {common:{mid:102},page:{page_id:good_list,last_page_id:detail},ts:300000}

 //mid101访问home之后没有访问其他页面，定义为跳出用户，
 //mid102在10秒内访问了其他用户定义为非跳出用户,最后的结果输出应该如下：
 {"common":{"mid":"101"},"page":{"page_id":"home"},"ts":10000}
 */
public class CepOfFlatSelect {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\cepjson.txt";
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("tag"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        KeyedStream<JSONObject, String> changeSouce = dataSource.map(json-> JSONObject.parseObject(json)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }
                )
        ).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return JSONObject.parseObject(jsonObject.getString("common")).getString("mid");
            }
        });

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String last_pageId = JSONObject.parseObject(jsonObject.getString("page")).getString("last_page_id");
                if (last_pageId == null || last_pageId.length() == 0) {
                    return true;
                }
                return false;
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String last_pageId = JSONObject.parseObject(jsonObject.getString("page")).getString("last_page_id");
                if (last_pageId != null || last_pageId.length() > 0) {
                    return true;
                }
                return false;
            }
        }).within(Time.milliseconds(10));

        PatternStream<JSONObject> pattern1Stream = CEP.pattern(changeSouce, pattern);
        SingleOutputStreamOperator<JSONObject> start = pattern1Stream.flatSelect(
                outputTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<JSONObject> collector) throws Exception {
                        List<JSONObject> start = map.get("start");
                        for (JSONObject json : start) {
//                            System.out.println(json);
                            collector.collect(json);
                        }
                    }
                }
                , new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {

                    }
                }
        );


        start.getSideOutput(outputTag).print("timeout");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
