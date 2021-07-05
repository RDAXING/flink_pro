package com.fahai.cc.tableAPI;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TableApi_CommonAPI {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        //方式1：调用老版本的planner的流处理
//        EnvironmentSettings build1 = EnvironmentSettings.newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build1);
//        //方式2：基于老版本的planner的批量处理
//        ExecutionEnvironment batchEnc = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnc);
//        //方式4,：基于blink的批处理
//        EnvironmentSettings build4 = EnvironmentSettings.newInstance().useBlinkPlanner()
//                .inBatchMode().build();
//        TableEnvironment batchBlinkEnv = TableEnvironment.create(build4);
//

        //方式3：基于blink的流处理
        EnvironmentSettings build3 = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkEnv = StreamTableEnvironment.create(env, build3);
        //读取文件中的数据
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\tables.txt";
        blinkEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                        .field("tyoe",DataTypes.STRING())
                        .field("date",DataTypes.STRING())
                        .field("num",DataTypes.INT())
                        .field("address",DataTypes.STRING())
                ).createTemporaryTable("inputTable");

        Table inputTable = blinkEnv.from("inputTable");
        blinkEnv.toAppendStream(inputTable, Row.class).print("blink-env");

        inputTable.printSchema();
        try {
            env.execute("blink-api");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testBlink(){
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\tables.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id",DataTypes.STRING())
                    .field("type",DataTypes.STRING())
                        .field("date",DataTypes.STRING())
                        .field("num",DataTypes.INT())
                        .field("address",DataTypes.STRING())
                ).createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        tableEnv.toAppendStream(inputTable,Row.class).print("table_env");


        try {
            env.execute("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取文件数据，然后对处理后的数据写出到文件
     */
    @Test
    public void readFile(){
        try {
        //读取文件地址
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\cep.txt";
        //输出到文件地址
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
            tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema().field("ip",DataTypes.STRING())
                    .field("name",DataTypes.STRING())
                        .field("url",DataTypes.STRING())
                        .field("date",DataTypes.STRING())
                ).createTemporaryTable("inputTable");

            Table inputTable = tableEnv.from("inputTable");
            Table name = inputTable.select("ip,name").filter("name === 'zhubajie'");

            String outputpath = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\cepout.txt";


            tableEnv.connect(new FileSystem().path(outputpath))
                        .withFormat(new Csv())
                        .withSchema(new Schema().field("ip",DataTypes.STRING())
                                .field("name",DataTypes.STRING())
                        ).createTemporaryTable("outTable");
    //
             name.insertInto("outTable");//写入数据到文件中

            tableEnv.execute("table");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void TableAPIReadKafka(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        tableEnv.connect(
                new Kafka()
                .version("0.11")
                .topic("BATCH_ES")
                .property("group.id", "BATCH_ES")
                .property("zookeeper.connect","cdh1.fahaicc.com:2181,cdh2.fahaicc.com:2181,cdh3.fahaicc.com:2181")
                .property("bootstrap.servers","cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092")
                .startFromGroupOffsets()
        ).withFormat(new Csv())
        .withSchema(new Schema()
                .field("ip",DataTypes.STRING())
                .field("name",DataTypes.STRING())
                .field("url",DataTypes.STRING())
                .field("date",DataTypes.STRING())
        ).createTemporaryTable("input");

        Table input = tableEnv.from("input").select("ip,name").filter("name === 'zhubajie'");
        tableEnv.toAppendStream(input,Row.class).print("table-input");



       //将处理后的数据推送到kafka指定的topic中
        tableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("test")
                        .property("zookeeper.connect","cdh1.fahaicc.com:2181,cdh2.fahaicc.com:2181,cdh3.fahaicc.com:2181")
                        .property("bootstrap.servers","cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092")
        ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("ip",DataTypes.STRING())
                        .field("name",DataTypes.STRING()))
                .createTemporaryTable("outTable");
        input.insertInto("outTable");
        try {
            tableEnv.execute("out-put");
//            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
