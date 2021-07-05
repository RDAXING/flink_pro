package com.fahai.cc.tableAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * dataStream 转换 table
 * table to dataStream
 */
public class FlinkTable1_datastreamToTable {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\tables.txt";
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");
        DataStream<Tuple5<String, String, String, String, String>> tranformaSource = dataSource.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple5<>(split[0], split[1], split[2], split[3], split[4]);
            }
        });

        //获取table的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //dataStream转换为table
        Table table = tableEnv.fromDataStream(tranformaSource);
        //对列名进行重命名
        Table resultTable = table.renameColumns("f0 as userId").select("userId,f1");
        resultTable.printSchema();
        //只获取2到3列的数据
        Table selectToTable = table.select("withColumns(2 to 3)");

        //dataStream转换为sql的形式
        tableEnv.createTemporaryView("person",tranformaSource);
        Table sqlTable = tableEnv.sqlQuery("select * from person");
        //table转换为dataSource
        tableEnv.toAppendStream(resultTable, Row.class).print("table");
        tableEnv.toAppendStream(sqlTable,Row.class).print("sql");
        tableEnv.toAppendStream(selectToTable,Row.class).print("select2to3");

        try {
            env.execute("table");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
