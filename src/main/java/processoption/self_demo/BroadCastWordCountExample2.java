package processoption.self_demo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.SeekableDataOutputView;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.util.concurrent.TimeUnit;
/**
 * 项目：flink_pro
 * 包名：processoption.self_demo
 * 作者：rdx
 * 日期：2021/7/28 16:31
 */
public class BroadCastWordCountExample2 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String value1 = "{'name':'xiao_wang','age':'10','id':'1','info':'进入'}";
        String value2 = "{'name':'xiao_wang','age':'10','id':'1','info':'退出'}";
        String value3 = "{'name':'xiao_wang','age':'10','id':'1','info':'购物'}";
        String value4 = "{'name':'xiao_wang','age':'10','id':'1','info':'收藏'}";


        String value5 = "{'name':'xiao_sang','age':'20','id':'2','info':'进入'}";
        String value6 = "{'name':'xiao_sang','age':'20','id':'2','info':'退出'}";
        String value7 = "{'name':'xiao_sang','age':'20','id':'2','info':'退出'}";

        String value8 = "{'name':'xiao_hai','age':'30','id':'3','info':'进入'}";
        String value9 = "{'name':'xiao_hai','age':'30','id':'3','info':'购物'}";
        String value10 = "{'name':'xiao_hai','age':'30','id':'3','info':'购物'}";


        JSONObject jsonObject1 = JSON.parseObject(value1);
        JSONObject jsonObject2 = JSON.parseObject(value2);
        JSONObject jsonObject3 = JSON.parseObject(value3);
        JSONObject jsonObject4 = JSON.parseObject(value4);

        JSONObject jsonObject5 = JSON.parseObject(value5);
        JSONObject jsonObject6 = JSON.parseObject(value6);
        JSONObject jsonObject7 = JSON.parseObject(value7);

        JSONObject jsonObject8 = JSON.parseObject(value8);
        JSONObject jsonObject9 = JSON.parseObject(value9);
        JSONObject jsonObject10 = JSON.parseObject(value10);

        List<JSONObject> list = new ArrayList<>();
        list.add(jsonObject1);
        list.add(jsonObject2);
        list.add(jsonObject3);
        list.add(jsonObject4);
        list.add(jsonObject5);
        list.add(jsonObject6);
        list.add(jsonObject7);
        list.add(jsonObject8);
        list.add(jsonObject9);
        list.add(jsonObject10);
        String broadCast1 = "{'condition_id':'1','firstAction':'进入','secondAction':'购物','topic':'进入+购物'}";
        String broadCast2 = "{'condition_id':'2','firstAction':'进入','secondAction':'退出','topic':'进入+退出'}";
        JSONObject broadCastJson1 = JSON.parseObject(broadCast1);
        JSONObject broadCastJson2 = JSON.parseObject(broadCast2);

        List<JSONObject> list2 = new ArrayList<>();
        list2.add(broadCastJson1);
        list2.add(broadCastJson2);


        final MapStateDescriptor<String, JSONObject> mapStateDes = new MapStateDescriptor<>(
                "state",
                String.class,
                JSONObject.class);

        // 自定义广播流（单例）
        BroadcastStream<JSONObject> broadcastStream = env.addSource(new RichSourceFunction<JSONObject>() {

            private volatile boolean isRunning = true;

            /**
             * 数据源：模拟每30秒随机更新一次拦截的关键字
             * @param ctx
             * @throws Exception
             */
            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {
                System.out.println("list2 = " + list2);
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(1);
                    Random random = new Random();

                    //todo 定时刷新，睡眠6秒

                    int i = random.nextInt(2);

                    ctx.collect(list2.get(i));
                    System.out.println("发送的字符串:" + list.get(i));

                }


            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).broadcast(mapStateDes);

        // 自定义数据流（单例）
        DataStream<JSONObject> dataStream = env.addSource(new RichSourceFunction<JSONObject>() {

            private volatile boolean isRunning = true;



            /**
             * 模拟每3秒随机产生1条消息
             * @param ctx
             * @throws Exception
             */
            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {

                while (isRunning) {
                    Random random = new Random();
                    TimeUnit.SECONDS.sleep(3);
                    int i = random.nextInt(10);
                    ctx.collect(list.get(i));
//                    System.out.println("kafka接收数据：" + dataSet[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }

        }).setParallelism(1);
        dataStream.keyBy(new KeySelector<JSONObject, String>() {

            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        }).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<String, JSONObject, JSONObject, String>() {

            private ValueState<String> prevActionState;

            //todo 状态
            private transient MapState<String, JSONObject> infoState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                prevActionState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>(
                                "lastAction",
                                String.class));


                infoState = getRuntimeContext().getMapState(
                        new MapStateDescriptor<String, JSONObject>(
                                "infoState",
                                String.class,
                                JSONObject.class));
            }


            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                JSONObject getBroad = ctx.getBroadcastState(mapStateDes).get("a");
                String currentKey = ctx.getCurrentKey();

                String prevAction = prevActionState.value();
                if (prevAction!=null ){
                    System.out.println("prevAction = " + prevAction);
                }
                if (infoState !=null){
                    System.out.println("currentKey:"+currentKey+",infoState = " + infoState.get("bbb"));
                }

                System.out.println("getBroad = " + getBroad);

                System.out.println("接收日志：" + value);
                infoState.put("bbb",getBroad);
                prevActionState.update(value.toString());

            }

            @Override
            public void processBroadcastElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(mapStateDes);

                broadcastState.put("a", value);
//                System.out.println("进入的广播变量：value = " + value);


            }


        });


        env.execute("BroadCastWordCountExample");


    }
}
