package com.fahai.cc.cep_stat_pro;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * author rdx
 *需求
 * 创建订单之后15分钟之内一定要付款，否则就取消订单
 * 订单数据格式如下类型字段说明
 * 订单编号
 * 订单状态 1.创建订单,等待支付 2.支付订单完成 3.取消订单，申请退款 4.已发货 5.确认收货，已经完成
 * 订单创建时间
 * 订单金额
 *
 * 数据样例：
 * 20160801000227050311955990,4,2016-07-29 12:21:12,165
 */
public class CepOforderCheck {

    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        OutputTag<Tuple4<String, String, String, Double>> outputTag = new OutputTag<Tuple4<String, String, String, Double>>("tag"){};
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\detail.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        KeyedStream<Tuple4<String, Integer, Long, Double>, String> keyedStream = dataSource.map(new MapFunction<String, Tuple4<String, Integer, Long, Double>>() {
            @Override
            public Tuple4<String, Integer, Long, Double> map(String s) throws Exception {
                String[] ss = s.split(",");
                long eventTime = format.parse(ss[2]).getTime();
//                Thread.sleep(3000);
                return new Tuple4<>(ss[0], Integer.parseInt(ss[1]), eventTime, Double.parseDouble(ss[3]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, Integer, Long, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Integer, Long, Double>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, Integer, Long, Double> t, long l) {
                        return t.f2;
                    }
                })).keyBy(f -> f.f0);

        Pattern<Tuple4<String, Integer, Long, Double>, Tuple4<String, Integer, Long, Double>> pattern = Pattern
                .<Tuple4<String, Integer, Long, Double>>begin("start")
                .where(new SimpleCondition<Tuple4<String, Integer, Long, Double>>() {
                    @Override
                    public boolean filter(Tuple4<String, Integer, Long, Double> t4) throws Exception {
                        if (t4.f1 == 1) {
                            return true;
                        }
                        return false;
                    }
        }).followedBy("second").where(new SimpleCondition<Tuple4<String, Integer, Long, Double>>() {
            @Override
            public boolean filter(Tuple4<String, Integer, Long, Double> t4) throws Exception {
                if (t4.f1 == 2) {
                    return true;
                }
                return false;
            }
        }).within(Time.minutes(15));


        PatternStream<Tuple4<String, Integer, Long, Double>> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<Tuple4<String, Integer, Long, Double>> selectSource = patternStream.select(outputTag,
                new PatternTimeoutFunction<Tuple4<String, Integer, Long, Double>, Tuple4<String, String, String, Double>>() {
                    @Override
                    public Tuple4<String, String, String, Double> timeout(Map<String, List<Tuple4<String, Integer, Long, Double>>> map, long l) throws Exception {
                        List<Tuple4<String, Integer, Long, Double>> start = map.get("start");
                        Tuple4<String, Integer, Long, Double> next = start.iterator().next();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        System.out.println("支付超时");
                        return new Tuple4<>("超时id:" + next.f0, "订单状态：" + next.f1, "起始-结束时间：" + sdf.format(new Date(next.f2) ), next.f3);
                    }
                },
                new PatternSelectFunction<Tuple4<String, Integer, Long, Double>, Tuple4<String, Integer, Long, Double>>() {
                    @Override
                    public Tuple4<String, Integer, Long, Double> select(Map<String, List<Tuple4<String, Integer, Long, Double>>> map) throws Exception {

                        Tuple4<String, Integer, Long, Double> second = map.get("second").iterator().next();
                        System.out.println("支付成功");
                        return second;
                    }
                }

        );

        selectSource.getSideOutput(outputTag).print("TimeOut");
        selectSource.print("success");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
