package com.fahai.cc.tableAPI;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.Serializable;

import static org.apache.flink.table.api.Expressions.*;

public class TableAPI_ProcessTime implements  Serializable {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }

    /**
     * table api 定义处理时间
     * 通过DataStream流转换为table 并指定处理时间 即：$("pt").proctime().as("processTime")
     */
    @Test
    public void  TableAPI_ProcessTime(){
        //rdx提交github
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\persones.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        SingleOutputStreamOperator<Tuple4<String, Long, Double, String>> dataSource = env.readTextFile(path, "UTF-8").map(new MapFunction<String, Tuple4<String, Long, Double, String>>() {
            @Override
            public Tuple4<String, Long, Double, String> map(String s) throws Exception {
                String[] ss = s.split(",");
                Thread.sleep(5);
                return new Tuple4<>(ss[0], FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(ss[1]).getTime(), Double.parseDouble(ss[2]), ss[3]);
            }
        });
        //定义处理时间字段
        Table table = tableEnv.fromDataStream(dataSource,$("f0").as("name"),$("f1").as("birthday"),$("f2").as("ht"),$("f3").as("address"),$("pt").proctime().as("processTime"));
//        Table table = tableEnv.fromDataStream(dataSource,"f0 as name,f3 as address,pt.proctime");
        table.printSchema();
        tableEnv.toAppendStream(table,Row.class).print("处理时间");

        Table windowGroupTable = table.window(Tumble.over("1.seconds").on("processTime").as("wt"))
                .groupBy($("name"), $("wt"))
                .select($("name"), $("name").count(), $("ht").avg(), $("wt").end());

        windowGroupTable.printSchema();
        tableEnv.toRetractStream(windowGroupTable,Row.class).print("windowGroupTable");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 事件时间的处理
     * 通过DataStream流转换为table 并指定事件时间
     * 注意三个点：
     * 1.设置时间类型：env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
     * 2.指定时间字段：assignTimestampsAndWatermarks
     * 3.添加新的一列为事件时间 ： $("rt").rowtime()
     *   指定源数据中某一列为事件时间：$("f1").rowtime().as("birthday")
     */
    @Test
    public void TableAPI_EventTime(){
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\persones.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        SingleOutputStreamOperator<Tuple4<String, Long, Double, String>> dataSource = env.readTextFile(path, "UTF-8").map(new MapFunction<String, Tuple4<String, Long, Double, String>>() {
            @Override
            public Tuple4<String, Long, Double, String> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple4<>(ss[0], FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(ss[1]).getTime(), Double.parseDouble(ss[2]), ss[3]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, Long, Double, String>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Long, Double, String>>() {
            @Override
            public long extractTimestamp(Tuple4<String, Long, Double, String> t, long l) {
                return t.f1;
            }
        }));


//        dataSource.print("meta");
        //定义事件时间字段
        Table eventTimeTable = tableEnv.fromDataStream(dataSource, $("f0").as("name"), $("f1").as("birthday"),$("rt").rowtime());
//        Table eventTimeTable = tableEnv.fromDataStream(dataSource, $("f0").as("name"), $("f1").rowtime().as("birthday"));
        eventTimeTable.printSchema();
        tableEnv.toAppendStream(eventTimeTable,Row.class).print("事件时间");

        try {
            env.execute("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 窗口操作
     * Group window
     *
     */
    @Test
    public void WindowGroupTable(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple4<String, Long, Double, String>> mapSource = dataSource.map(new MapFunction<String, Tuple4<String, Long, Double, String>>() {
            @Override
            public Tuple4<String, Long, Double, String> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple4<>(ss[0], FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(ss[1]).getTime(), Double.parseDouble(ss[2]), ss[3]);

            }
        });

        Table table = tableEnv.fromDataStream(mapSource,$("f0").as("name"),$("f1").as("birthday"),$("f2").as("ht"),$("f3").as("address"),$("pt").proctime().as("processTime"));
        table.printSchema();
        tableEnv.toAppendStream(table,Row.class).print("处理时间");

        /*Table API*/
        Table windowGroupTable = table.window(Tumble.over(lit(10).seconds()).on($("processTime")).as("wt"))
                .groupBy($("name"), $("wt"))
                .select($("name"), $("name").count(), $("ht").avg(), $("wt").end());

        windowGroupTable.printSchema();
        tableEnv.toRetractStream(windowGroupTable,Row.class).print("windowGroupTable");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void WindowGroupOfEventTime(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        SingleOutputStreamOperator<Tuple4<String, Long, Double, String>> dataSource = env.socketTextStream("localhost", 6666).map(new MapFunction<String, Tuple4<String, Long, Double, String>>() {
            @Override
            public Tuple4<String, Long, Double, String> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple4<>(ss[0], FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(ss[1]).getTime(), Double.parseDouble(ss[2]), ss[3]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, Long, Double, String>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Long, Double, String>>() {
            @Override
            public long extractTimestamp(Tuple4<String, Long, Double, String> t, long l) {
                return t.f1;
            }
        }));


        Table eventTimeTable = tableEnv.fromDataStream(dataSource, $("f0").as("name"), $("f1").as("birthday"),$("f2").as("height"),$("f3").as("address"),$("rt").rowtime());
        eventTimeTable.printSchema();
        tableEnv.toAppendStream(eventTimeTable,Row.class).print("事件时间");

        Table overTable = eventTimeTable.window(Over.partitionBy($("name")).orderBy($("rt")).preceding("2.rows").as("ow"))
                .select("name,name.count over ow,height.avg over ow ");
//                .select($("name"), $("name").count().over("ow"), $("height").avg().over("ow"));
        tableEnv.toAppendStream(overTable,Row.class).print("overWindow");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testTop2(){
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\persones.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        SingleOutputStreamOperator<Tuple4<String, Long, Integer, String>> dataSource = env.readTextFile(path, "UTF-8").map(new MapFunction<String, Tuple4<String, Long, Integer, String>>() {
            @Override
            public Tuple4<String, Long, Integer, String> map(String s) throws Exception {
                String[] ss = s.split(",");
                Thread.sleep(5);
                return new Tuple4<>(ss[0], FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(ss[1]).getTime(), Integer.parseInt(ss[2]), ss[3]);
            }
        });
        Table table = tableEnv.fromDataStream(dataSource);
        tableEnv.toAppendStream(table,Row.class).print();

        table.printSchema();
        tableEnv.registerFunction("top2",new Top2());
        Table resultTop2Table = table.flatAggregate(call("top2", $("f2")).as("v", "rank"))
                .select($("v"), $("rank"));

        tableEnv.toRetractStream(resultTop2Table,Row.class).print("top2");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Accumulator for Top2.
     */
    public class Top2Accum {
        public Integer first;
        public Integer second;
    }

    /**
     * The top2 user-defined table aggregate function.
     */
    public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

        @Override
        public Top2Accum createAccumulator() {
            Top2Accum acc = new Top2Accum();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }


        public void accumulate(Top2Accum acc, Integer v) {
            if (v > acc.first) {
                acc.second = acc.first;
                acc.first = v;
            } else if (v > acc.second) {
                acc.second = v;
            }
        }

        public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
            for (Top2Accum otherAcc : iterable) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}
