package com.fahai.cc.tableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableAPI_WriteDataToES {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        /*写入数据到elasticsearch1*/
        //1.读取文件数据路径
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\persones.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        int parallelism = env.getParallelism();
        System.out.println("当前分区数：" +parallelism);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        //进行文件的读取
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("pname", DataTypes.STRING())
                        .field("birth",DataTypes.STRING())
                        .field("height",DataTypes.DOUBLE())
                        .field("address",DataTypes.STRING())
                ).createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        /******************************标量函数----start************************************/
        //自定义函数之标量函数 注册方式1：在 Table API 里不经注册直接“内联”调用函数
        Table innFuTable = inputTable.select(call(HashFunction.class,$("pname")).as("pnameHash"),$("address"));
        innFuTable.printSchema();
        tableEnv.toAppendStream(innFuTable,Row.class).print("function1");

        //自定义函数之标量函数 注册方式2：createTemporarySystemFunction
        tableEnv.createTemporarySystemFunction("hashFunction",HashFunction.class);
        Table selectFunTable = inputTable.select(call("hashFunction", $("pname")).as("name2"), $("birth"), $("address"));
        selectFunTable.printSchema();
        tableEnv.toAppendStream(selectFunTable,Row.class).print("function2");

        /******************************标量函数----end************************************/


        /******************************表值函数----start************************************/

        //自定义函数之表值函数
        // 在 Table API 里不经注册直接“内联”调用函数
        //joinLateral / leftOuterJoinLateral 算子会把外表（算子左侧的表）的每一行跟跟表值函数返回的所有行（位于算子右侧）进行 join
        Table tableFunctionTs = inputTable
//                .joinLateral(call(FieldAndLengthFunction.class, $("pname")))
                .leftOuterJoinLateral(call(FieldAndLengthFunction.class, $("pname")))
                .select($("pname"), $("field"), $("length"));

        tableEnv.toAppendStream(tableFunctionTs,Row.class).print("内联调用表值函数");

        // 先注册函数 在 Table API 里调用注册好的函数
        tableEnv.createTemporarySystemFunction("fieldLength",FieldAndLengthFunction.class);

        Table tableFunctionTs2 = inputTable
//                .joinLateral(call("fieldLength", $("pname")))
                .leftOuterJoinLateral(call("fieldLength",$("pname")))
                .select($("field"), $("length"), $("address"), $("birth"));

        tableEnv.toAppendStream(tableFunctionTs2,Row.class).print("注册调用表值函数");

        /******************************表值函数-----end************************************/
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 标量函数
     */
    public static class HashFunction extends ScalarFunction{
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
            return o.hashCode();
        }
    }

    /**
     * 表值函数
     *
     */
    @FunctionHint(output = @DataTypeHint("Row<field STRING,length INT>"))
    public static class FieldAndLengthFunction extends TableFunction<Row>{
        public void eval(String field){
            collect(Row.of(field,field.length()));
        }
    }
}


