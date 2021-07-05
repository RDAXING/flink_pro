package com.fahai.cc.tableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.Mod;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * rdx
 * 聚合函数的使用
 */
public class TableAPI_AggregateFunction {
    static {
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\aggregate.txt";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id", DataTypes.INT())
                        .field("product",DataTypes.STRING())
                        .field("price",DataTypes.BIGINT())
                ).createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        tableEnv.registerFunction("wAvg",new SumAgg());
        inputTable.select($("").mod($("")));
        Table priceSum = inputTable.groupBy($("product")).select($("product"),call("wAvg", $("price")));
        tableEnv.toRetractStream(priceSum,Row.class).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class SumAgg extends AggregateFunction<Long,WeightedAvgAccum>{


        /**
         * 聚合的最终结果类型
         * @param acc
         * @return
         */
        @Override
        public Long getValue(WeightedAvgAccum acc) {

            return acc.sum;
        }

        /**
         * 聚合期间的中间结果类型
         * @return
         */
        @Override
        public WeightedAvgAccum createAccumulator() {
            WeightedAvgAccum weightedAvgAccum = new WeightedAvgAccum();
            weightedAvgAccum.sum = 0l;
            return weightedAvgAccum;
        }

        /**
         * 计算逻辑的处理
         */

        public void accumulate(WeightedAvgAccum acc,Long price){
            acc.sum += price;
        }
    }
    public static class WeightedAvgAccum{
        public Long sum;
    }
}
