package processoption.self_demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 项目：flink_pro
 * 包名：processoption.self_demo
 * 作者：rdx
 * 日期：2021/7/28 16:07
 * 广播流练习：
 * 读取两个文件流:
 * 一个为广播流:从socket流中获取动态数据
 * 一个为普通流：可以从kafka中获取也可以从其他获取
 * 通过connect进行连接
 */
public class ConnBroadCastProcessFunction {
    public static void main(String[] args) {
        //创建黑名单测流标记
        OutputTag<Tuple2<String, String>> tag = new OutputTag<Tuple2<String, String>>("backList") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //使用 MapStateDescriptor 来描述并创建 broadcast state 在下游的存储结构
        MapStateDescriptor<String, Set<String>> mapBroadcast = new MapStateDescriptor<>("broadcase", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Set<String>>() {
        }));
        SingleOutputStreamOperator<Set<String>> broadCaseConf = env.socketTextStream("localhost", 6666).flatMap(new FlatMapFunction<String, Set<String>>() {
            @Override
            public void flatMap(String s, Collector<Set<String>> collector) throws Exception {
                String[] line = s.split(",");
                collector.collect(new HashSet<>(Arrays.asList(line)));
            }
        });
        BroadcastStream<Set<String>> broadcastDataSource = broadCaseConf.broadcast(mapBroadcast);

        //广播流与普通流通过connect进行连接
        SingleOutputStreamOperator<String> processBroadSource = getDataSource(env).connect(broadcastDataSource).process(new BroadcastProcessFunction<String, Set<String>, String>() {
//            MapStateDescriptor<String, String> mapBroadcast = new MapStateDescriptor<>("broadcase", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

            MapStateDescriptor<String, Set<String>> mapBroadcast = new MapStateDescriptor<>("broadcase", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Set<String>>() {
            }));

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
//                String broadCast = ctx.getBroadcastState(mapBroadcast).get("a");
                Set<String> broadCast = ctx.getBroadcastState(mapBroadcast).get("a");
                if (null == broadCast) {
                    broadCast = new HashSet<String>();
                    broadCast.add("java");
                }
                StringBuffer sb = new StringBuffer();
                for (String b : broadCast) {
                    sb.append(b).append(",");

                }
                if(broadCast.contains(value)){
                    ctx.output(tag,new Tuple2<>(sb.toString(),value));
                }else{
                    out.collect(value);
                }
            }

            @Override
            public void processBroadcastElement(Set<String> value, Context ctx, Collector<String> out) throws Exception {
                //获取广播流中的数据，ctx.getBroadcastState(mapBroadcast)中的参数要
                // 与创建广播流的状态参数一致broadCaseConf.broadcast(mapBroadcast);
//                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapBroadcast);
//                System.out.println("-----------------输入黑名单：" + value);

                BroadcastState<String, Set<String>> broadcastState = ctx.getBroadcastState(mapBroadcast);

                //key随机设置的值
                broadcastState.put("a", value);

            }
        });

        processBroadSource.getSideOutput(tag).print("黑名单");
        processBroadSource.print("正常数据");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataStreamSource<String> getDataSource(StreamExecutionEnvironment env){
        // 自定义数据流（单例）
        return env.addSource(new RichSourceFunction<String>() {

            private volatile boolean isRunning = true;

            //测试数据集
            private String[] dataSet = new String[]{
                    "java",
                    "spark",
                    "scala",
                    "hive",
                    "hbase",
                    "kafka",
                    "flink",
                    "mysql"
            };

            /**
             * 模拟每3秒随机产生1条消息
             * @param ctx
             * @throws Exception
             */
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = dataSet.length;
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(3);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(dataSet[seed]);
                    System.out.println("读取到上游发送的消息：" + dataSet[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }

        });
    }
}
