package com.fahai.cc.project1_leftjoin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fahai.cc.project1_leftjoin.pojo.OrderDetailBean;
import com.fahai.cc.project1_leftjoin.pojo.OrderMainBean;
import com.fahai.cc.project1_leftjoin.utils.FlinkUtilsV2;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.*;


public class OrderJoinAdv {
    public static void main(String[] args) throws Exception {
        //获取配置文件
        ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = FlinkUtilsV2.getEnv();

        // 使用EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 一、从kafka中加载数据源
        DataStream<String> orderMainLines = FlinkUtilsV2.createKafkaDataStream(propertiesFile, "ordermain",
                "g011", SimpleStringSchema.class);

        DataStream<String> orderDetailLines = FlinkUtilsV2.createKafkaDataStream(propertiesFile, "orderdetail",
                "g011", SimpleStringSchema.class);

        //  二、对从kafka拉取到的json数据，进行解析

        // 加载OrderMain中数据
        SingleOutputStreamOperator<OrderMainBean> orderMainBeanDS = orderMainLines.process(new ProcessFunction<String,
                OrderMainBean>() {
            @Override
            public void processElement(String input, Context ctx, Collector<OrderMainBean> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(input);
                    String type = jsonObject.getString("type");
                    if (type.equals("INSERT") || type.equals("UPDATE")) {

                        //将data[]数组的{}中内容取出来
                        JSONArray jsonArray = jsonObject.getJSONArray("data");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            OrderMainBean orderMain = jsonArray.getObject(i, OrderMainBean.class);
                            orderMain.setType(type); //设置操作类型
                            out.collect(orderMain);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    //TODO 记录错误数据
                }
            }
        });


        //加载OrderDetail中数据
        SingleOutputStreamOperator<OrderDetailBean> orderDetailBeanDS =
                orderDetailLines.process(new ProcessFunction<String,
                        OrderDetailBean>() {
                    @Override
                    public void processElement(String input, Context ctx, Collector<OrderDetailBean> out) throws Exception {

                        try {
                            JSONObject jsonObject = JSON.parseObject(input);
                            String type = jsonObject.getString("type");
                            if (type.equals("INSERT") || type.equals("UPDATE")) {
                                JSONArray jsonArray = jsonObject.getJSONArray("data");
                                for (int i = 0; i < jsonArray.size(); i++) {
                                    OrderDetailBean orderDetail = jsonArray.getObject(i, OrderDetailBean.class);
                                    orderDetail.setType(type); //设置操作类型
                                    out.collect(orderDetail);
                                }
                            }
                        } catch (Exception e) {
                            //e.printStackTrace();
                            //记录错误的数据
                        }
                    }
                });

        int delaySeconds = 2;
        int windowSize = 5;

        SingleOutputStreamOperator<OrderMainBean> orderMainWithWaterMark =
                orderMainBeanDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor
                        <OrderMainBean>(Time.seconds(delaySeconds)) {
                    @Override
                    public long extractTimestamp(OrderMainBean element) {
                        return element.getCreate_time().getTime();
                    }
                });

        SingleOutputStreamOperator<OrderDetailBean> orderDetailWithWaterMark
                =
                orderDetailBeanDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor
                        <OrderDetailBean>(Time.seconds(delaySeconds)) {
                    @Override
                    public long extractTimestamp(OrderDetailBean element) {
                        return element.getCreate_time().getTime();
                    }
                });

        DataStream<Tuple2<OrderDetailBean, OrderMainBean>> joined =
                orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                        .where(new KeySelector<OrderDetailBean, Long>() {
                            @Override
                            public Long getKey(OrderDetailBean value) throws Exception {
                                return value.getOrder_id();
                            }
                        })
                        .equalTo(new KeySelector<OrderMainBean, Long>() {
                            @Override
                            public Long getKey(OrderMainBean value) throws Exception {
                                return value.getOid();
                            }
                        })
                        .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                        .apply(new CoGroupFunction<OrderDetailBean, OrderMainBean, Tuple2<OrderDetailBean,
                                OrderMainBean>>() {

                            // 做join之后，输出的结果
                            @Override
                            public void coGroup(Iterable<OrderDetailBean> first, Iterable<OrderMainBean> second,
                                                Collector<Tuple2<OrderDetailBean, OrderMainBean>> out) throws Exception {
                                for (OrderDetailBean orderDetailBean : first) {

                                    boolean isJoined = false;
                                    for (OrderMainBean orderMainBean : second) {
                                        out.collect(Tuple2.of(orderDetailBean, orderMainBean));

                                        isJoined = true;
                                    }

                                    if (!isJoined) {
                                        out.collect(Tuple2.of(orderDetailBean, null));
                                    }
                                }
                            }
                        });
        OutputTag<OrderDetailBean> outputTag = new OutputTag<OrderDetailBean>("leftLate-date") {
        };

        // 划分窗口，和上面join的窗口大小和类型保持一致
        SingleOutputStreamOperator<OrderDetailBean> orderDetailLateData =
                orderDetailWithWaterMark.windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                        .sideOutputLateData(outputTag)
                        .apply(new AllWindowFunction<OrderDetailBean, OrderDetailBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<OrderDetailBean> values,
                                              Collector<OrderDetailBean> out) throws Exception {
                                //什么都不用操作，只是为迟到的数据打上标签
                                // 之所以用apply，因为要在窗口内的全量数据做操作
                            }
                        });

        // 拿出左表迟到数据
        DataStream<OrderDetailBean> leftLateDate = orderDetailLateData.getSideOutput(outputTag);


        // 查库，关联右表（OrderMain）数据
        SingleOutputStreamOperator<Tuple2<OrderDetailBean, OrderMainBean>> lateOrderDetailAndOrderMain =
                leftLateDate.map(new RichMapFunction<OrderDetailBean, Tuple2<OrderDetailBean, OrderMainBean>>() {

                    private transient Connection conn = null;

                    // 创建JDBC连接
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection(
                                "jdbc:mysql://linux04:3306/doit12?characterEncoding=utf8",
                                "root",
                                "123456"
                        );
                    }

                    //查询数据库
                    @Override
                    public Tuple2<OrderDetailBean, OrderMainBean> map(OrderDetailBean value) throws Exception {
                        Long order_id = value.getOrder_id();
                        String type = value.getType();
                        OrderMainBean orderMainB = queryOrderMainFromMySQL(order_id, type, conn);

                        return Tuple2.of(value, orderMainB);
                    }

                    // 关闭JDBC连接
                    @Override
                    public void close() throws Exception {
                        conn.close();
                    }
                });


        SingleOutputStreamOperator<Tuple2<OrderDetailBean, OrderMainBean>> lateOrderMainAndOrderMain =
                joined.map(new RichMapFunction<Tuple2<OrderDetailBean, OrderMainBean>, Tuple2<OrderDetailBean,
                        OrderMainBean>>() {

                    private transient Connection conn = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection(
                                "jdbc:mysql://linux04:3306/doit12?characterEncoding=utf8",
                                "root",
                                "123456"
                        );
                    }

                    //查询数据库  根据左表（orderDetail）的查询条件，查询右表（orderMain）的数据
                    @Override
                    public Tuple2<OrderDetailBean, OrderMainBean> map(Tuple2<OrderDetailBean, OrderMainBean> value) throws Exception {
                        OrderMainBean orderMainB = null;
                        if (value.f1 == null) {
                            Long order_id = value.f0.getOrder_id();
                            String type = value.f0.getType();
                            orderMainB = queryOrderMainFromMySQL(order_id, type, conn);
                        }
                        return Tuple2.of(value.f0, orderMainB);
                    }

                    @Override
                    public void close() throws Exception {
                        conn.close();
                    }
                });

        DataStream<Tuple2<OrderDetailBean, OrderMainBean>> allOrderStream =
                lateOrderMainAndOrderMain.union(lateOrderDetailAndOrderMain);


        allOrderStream.print();

        FlinkUtilsV2.getEnv().execute("OrderJoinAdv");
    }


    private static OrderMainBean queryOrderMainFromMySQL(Long order_id, String type, Connection conn) throws Exception {

        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;

        String sql = "select oid, create_time, total_money, status, update_time, province, uid from ordermain" +
                " where oid = ?";
        preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setLong(1, order_id);
        resultSet = preparedStatement.executeQuery();

        long oid = resultSet.getLong("oid");
        Date create_time = resultSet.getDate("create_time");
        double total_money1 = resultSet.getDouble("total_money");
        int status = resultSet.getInt("status");
        Date update_time = resultSet.getDate("update_time");
        String province = resultSet.getString("province");
        String uid = resultSet.getString("uid");

        OrderMainBean orderMain = new OrderMainBean(oid, create_time, total_money1, status, update_time,
                province, uid, type);

        return orderMain;
    }

}
