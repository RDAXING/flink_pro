package state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
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
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MyListState {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true);
        conf.setString("taskmanager.numberOfTaskSlots","12");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.enableCheckpointing(1000l);
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
        dataSource.flatMap(new OperatorStateRecoveryRichFunction()).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class OperatorStateRecoveryRichFunction extends RichFlatMapFunction<String, Tuple2<Integer,String>> implements CheckpointedFunction{
        //原始状态
        private List<String> listBuffer = new ArrayList<>();
        private transient ListState<String> checkPointList;
        @Override
        public void flatMap(String s, Collector<Tuple2<Integer, String>> collector) throws Exception {
            if(s.equals("hainiu")){
                if(listBuffer.size() > 0){
                    for(String ss : listBuffer){
                        StringBuffer sc = new StringBuffer();
                        if(sc.length() ==0){
                            sc.append(ss);
                        }else{
                            sc.append(" ").append(ss);
                        }
                        collector.collect(Tuple2.of(listBuffer.size(),sc.toString()));
                        listBuffer.clear();
                    }
                }
            }else if(s.equals("e")){
                System.out.println(1/0);
            }else{
                listBuffer.add(s);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkPointList.clear();
            for(String s : listBuffer){
                checkPointList.add(s);

            }
            System.out.println("保存数据到检测点:" + listBuffer.toString());
        }

        @Override
        public void initializeState(FunctionInitializationContext cox) throws Exception {
            checkPointList = cox.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                    "operate-state",
                    TypeInformation.of(new TypeHint<String>() {
                    })
            ));
            if(cox.isRestored()){
                for(String s :checkPointList.get()){
                    listBuffer.add(s);
                }
            }
            System.out.println("从检测点恢复数据:"+listBuffer.toString());
        }
    }
}
