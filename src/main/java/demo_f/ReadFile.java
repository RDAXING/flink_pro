package demo_f;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class ReadFile {
    public static void main(String[] args) {
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\a.txt";
        //获取文件中的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.readTextFile(path);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumSorce = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] ss = s.split(" ");
                for (String value : ss) {
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
            public String getKey(Tuple2<String, Integer> tup) throws Exception {
                return tup.f0;
            }
        }).sum(1);

        sumSorce.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过flink connector处理数据
     */
    @Test
    public void getMysqldata(){
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        String userName = "root";
        String password = "";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JDBCInputFormat jdbcconn = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverName)
                .setDBUrl(url)
                .setUsername(userName)
                .setPassword(password)
                .setQuery("select dataTaskId,dataTaskName from datataskinfor_bak")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                .finish();


        DataStreamSource<Row> input = env.createInput(jdbcconn);
        input.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过自定义的方式获取mysql中的数据
     */
@Test
    public void getMysqlOnwer(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Map<String, Object>> mysqlSource = env.addSource(new MysqlSourceFunction(), "mysqlSource");
        mysqlSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
class MysqlSourceFunction extends RichSourceFunction<Map<String,Object>>{
    private  Connection conn = null;
    private PreparedStatement ps = null;
    private static  String driverName = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    private static String userName = "root";
    private static String password = "";

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取mysql中的连接信息
//        String sql = "select * from datataskinfor_bak";
        String sql = "select dataTaskId,dataTaskName from datataskinfor_bak";
        ps = getConnection().prepareStatement(sql);


    }

    @Override
    public void run(SourceContext<Map<String, Object>> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while(resultSet.next()){
            HashMap<String, Object> result = new HashMap<>();
            for(int i=0;i<columnCount;i++){
                String field = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(i + 1);
                result.put(field,value);
            }

            sourceContext.collect(result);
        }

    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        if(ps != null){
            ps.close();
        }
        if(conn != null){
            conn.close();
        }
    }

    public static Connection  getConnection(){
        Connection conn = null;
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  conn;
    }
}


