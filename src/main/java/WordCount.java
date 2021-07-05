import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SplittableIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

public class WordCount {
    private static Logger logger = Logger.getLogger(WordCount.class);
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\a.txt";
        DataSource<String> stringDataSource = executionEnvironment.readTextFile(inpath);
        FlatMapOperator<String, String> fmo = stringDataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String value : s.split(" ")) {
                    collector.collect(value);
                }
            }
        });

        MapOperator<String, Tuple2<String, Integer>> map = fmo.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        });
        AggregateOperator<Tuple2<String, Integer>> sum = map.groupBy(0).sum(1);
        System.out.println(sum);
        try {
            sum.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 内存中读取数据fromElements的使用
     */
    @Test
    public void getDataFromMemory(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = executionEnvironment.fromElements("a v b d a x c x c");
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultSouce = dataSource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String field : s.split(" ")) {
                    collector.collect(field);
                }
            }
        })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {

                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).keyBy(0).sum(1);
        resultSouce.print();

        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * fromCollection的使用
     */
    @Test
    public void getElementsCO(){
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        String[] split = "a,v,gb,c,d,s,a,xz,x,f,z,w,a".split(",");
        DataStreamSource<String> dss = executionEnvironment.fromCollection(Arrays.asList(split));
        System.out.println("+++++++++++"+dss.getParallelism()+"+++++++++++++++");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dss.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(0).sum(1);
        sum.print();
        System.out.println();
        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getFromElementsParall(){
        // 创建实时计算的一个执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建抽象的数据集【创建原始的抽象数据集的方法，Source】
        DataStreamSource<Long> nums = environment.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println("=========" + nums.getParallelism() + "=========");
        // DataStream<Integer> nums = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        SingleOutputStreamOperator<Long> sum = nums.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });
        sum.print();
        try {
            environment.execute("SourceDemo2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取文件readTextFile
     */
    @Test
    public void getReadTxtFile(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo";
        DataStreamSource<String> dss = env.readTextFile(inpath, "UTF-8");
        dss.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dss.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String value : s.split(" ")) {
                    collector.collect(value);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(0).sum(1);

        sum.print();
        System.out.println(env.getExecutionPlan());
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getReadFile(){
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readFile(new TextInputFormat(null), inpath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dss.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String value : s.split(" ")) {
                    collector.collect(value);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> f) throws Exception {
                return f.f0;
            }
        }).sum(1);
        sum.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取socket中的数据
     */
    @Test
    public void getSocket(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(ss);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(0).sum(1);
        sum.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void getTimeWindowDemo(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String ss : split) {
                    collector.collect(ss);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {

                return new Tuple2<>(s, 1);
            }
        });
        // 将数据用5秒的滚动窗口做划分，再用ProcessWindowFunction
        SingleOutputStreamOperator<String> mainDataStream = dataStream
                // 以Tuple2的f0字段作为key，本例中实际上key只有aaa和bbb两种
                .keyBy(value -> value.f0)
                // 5秒一次的滚动窗口
                .timeWindow(Time.seconds(5))
                // 统计每个key当前窗口内的元素数量，然后把key、数量、窗口起止时间整理成字符串发送给下游算子
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {

                    // 自定义状态
                    private ValueState<KeyCount> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态，name是myState
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", KeyCount.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {

                        // 从backend取得当前单词的myState状态
                        KeyCount current = state.value();

                        // 如果myState还从未没有赋值过，就在此初始化
                        if (current == null) {
                            current = new KeyCount();
                            current.key = s;
                            current.count = 0;
                        }

                        int count = 0;

                        // iterable可以访问该key当前窗口内的所有数据，
                        // 这里简单处理，只统计了元素数量
                        for (Tuple2<String, Integer> tuple2 : iterable) {
//                            count++;
                            count+=tuple2.f1;
                        }

                        // 更新当前key的元素总数
                        current.count += count;

                        // 更新状态到backend
                        state.update(current);

                        // 将当前key及其窗口的元素数量，还有窗口的起止时间整理成字符串
                        String value = String.format("window, %s, %s - %s, %d,    total : %dn",
                                // 当前key
                                s,
                                // 当前窗口的起始时间
                                time(context.window().getStart()),
                                // 当前窗口的结束时间
                                time(context.window().getEnd()),
                                // 当前key在当前窗口内元素总数
                                count,
                                // 当前key出现的总数
                                current.count);

                        // 发射到下游算子
                        collector.collect(value);
                    }
                });

        // 打印结果，通过分析打印信息，检查ProcessWindowFunction中可以处理所有key的整个窗口的数据
        mainDataStream.print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String time(long timeStamp) {
        return new SimpleDateFormat("hh:mm:ss").format(new Date(timeStamp));
    }

    static class KeyCount {
        /**
         * 分区key
         */
        public String key;

        /**
         * 元素总数
         */
        public long count;
    }
    /**
     * addSource 和reduce的使用
     */
    @Test
    public void getAddSourceAndReduceOption(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, String, Integer>> tup3DS = env.addSource(new SourceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple3<String, String, Integer>> sourceContext) throws Exception {
                sourceContext.collect(new Tuple3<>("zs", "math", 98));
                sourceContext.collect(new Tuple3<>("zs", "china", 44));
                sourceContext.collect(new Tuple3<>("zs", "enlish", 65));
                sourceContext.collect(new Tuple3<>("ls", "math", 25));
                sourceContext.collect(new Tuple3<>("ls", "china", 66));
                sourceContext.collect(new Tuple3<>("ls", "enlish", 88));
            }

            @Override
            public void cancel() {

            }
        }, "source1");

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resCore = tup3DS.keyBy(0).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> v1, Tuple3<String, String, Integer> t1) throws Exception {

                return Tuple3.of(v1.f0, "总分", t1.f2 + v1.f2);
            }
        });

        resCore.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * reduce and keyby
     */
    @Test
    public void getReduceKeyBy(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<String, String, Integer>> tuple3s = new ArrayList<>();
        tuple3s.add(Tuple3.of("zhangsan","f",12));
        tuple3s.add(Tuple3.of("zhangsan","m",23));
        tuple3s.add(Tuple3.of("wangwu","f",42));
        tuple3s.add(Tuple3.of("zhaoer","m",32));
        tuple3s.add(Tuple3.of("wangwu","m",33));
        DataStreamSource<Tuple3<String, String, Integer>> dataSource = env.fromCollection(tuple3s);
        KeyedStream<Tuple3<String, String, Integer>, String> keyStream = dataSource.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> tu) throws Exception {
                return tu.f0;
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> res = keyStream.maxBy(2);
        res.print();
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> reduceS = keyStream.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> v1, Tuple3<String, String, Integer> t1) throws Exception {

                return Tuple3.of(v1.f0, "==>", v1.f2 + t1.f2);
            }
        });


        reduceS.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * connect 要和CoMapFunction或者CoFlatMapFunction搭配使用
     */
    @Test
    public void getConnectAndUnion(){
        StreamExecutionEnvironment enc = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> input1 = enc.generateSequence(0, 10);
        DataStreamSource<String> input2 = enc.fromElements("hahah jajaj lalal");
        ConnectedStreams<Long, String> connect = input1.connect(input2);

        SingleOutputStreamOperator<String> mapDS = connect.map(new CoMapFunction<Long, String, String>() {
            @Override
            public String map1(Long aLong) throws Exception {
                return aLong.toString() + "change";
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        });

        mapDS.print();
        /*
        * 6> 5change
        5> 4change
        2> 1change
        2> 9change
        4> 3change
        7> 6change
        1> 0change
        7> hahah jajaj lalal
        3> 2change
        8> 7change
        1> 8change
        3> 10change
        * */

        DataStreamSource<Long> ds1 = enc.generateSequence(11, 15);
        DataStreamSource<Long> ds2 = enc.generateSequence(16, 20);
        input1.union(ds1).union(ds2).print();


        DataStreamSource<String> dss1 = enc.fromElements("java spar scala");
        DataStreamSource<String> dss2 = enc.fromElements("hbase hive kafka");
        SingleOutputStreamOperator<String> flatmapDS = dss1.connect(dss2).flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String s, Collector<String> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(ss);
                }
            }

            @Override
            public void flatMap2(String s, Collector<String> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(ss);
                }
            }
        });

        flatmapDS.print();
        try {
            enc.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * split and select
     */
    @Test
    public void getSplistAndSelect(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataSource = env.generateSequence(1, 30);
        SplitStream<Long> splitSource = dataSource.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long aLong) {
                List<String> lists = new ArrayList<>();
                if (aLong % 2 == 0) {
                    lists.add("out1");

                } else {
                    lists.add("out2");
                }
                return lists;
            }
        });

        splitSource.select("out1").map(new MapFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "偶数：" + aLong;
            }
        }).print();
        SingleOutputStreamOperator<String> out2 = splitSource.select("out2").map(new MapFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "基数" + aLong;
            }
        });

        out2.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测流输出 sideoutput
     */
    @Test
    public void getSideOutPut(){
        OutputTag<String> outputTag1 = new OutputTag<String>("side-output-1"){};
        OutputTag<String> outputTag2 = new OutputTag<String>("side-output-2"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataSource = env.generateSequence(1, 30);
        SingleOutputStreamOperator<Long> process = dataSource.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long aLong, Context context, Collector<Long> collector) throws Exception {
                if(aLong %2 ==0){

                    context.output(outputTag1,"sideout-偶数：" + String.valueOf(aLong));
                }else{
                    context.output(outputTag2,"sideout-基数：" + String.valueOf(aLong));
                }
                collector.collect(aLong);
            }
        });

        process.getSideOutput(outputTag1).print();
        process.getSideOutput(outputTag2).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}




