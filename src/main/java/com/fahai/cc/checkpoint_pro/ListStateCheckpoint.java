package com.fahai.cc.checkpoint_pro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ListStateCheckpoint {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //设置容错模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(2000l);
        checkpointConfig.setCheckpointTimeout(10l);
        //保留策略
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StateBackend stateBackend = new FsStateBackend("hdfs://cdh2.fahaicc.com:8020/user/hue/tmp/casenoofpname/statfile", true);
        env.setStateBackend(stateBackend);
        //恢复策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, Time.of(0, TimeUnit.SECONDS)
        ));


        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        dataSource.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple2<>(ss[0],ss[1]);
            }
        }).keyBy(f->f.f0)
                .process(new MyKeyedProcessFunction()).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String,String>, Tuple2<String,List<String>>> implements CheckpointedFunction{


        private transient ListState<String> listState;
        private List<String> tmpList;


        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            String name = value.f1;
            Iterable<String> historyName = listState.get();
            tmpList = new ArrayList<>();
            if(historyName!= null){
                for(String ss:historyName){
                    tmpList.add(ss);
                }
            }
            tmpList.add(name);
//            listState.addAll(tmpList);
            listState.update(tmpList);
            out.collect(new Tuple2<>(value.f0,tmpList));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            for(String ss: tmpList){
                listState.add(ss);
            }
            System.out.println("保存数据到检测点:" + tmpList.toString());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getKeyedStateStore().getListState(new ListStateDescriptor<String>("check-state",TypeInformation.of(new TypeHint<String>() {
            })));
            if(context.isRestored()){
                for(String s: listState.get()){
                    listState.add(s);
                }
                System.out.println("从检测点恢复数据:"+listState.get().toString());
            }
        }
    }
}
