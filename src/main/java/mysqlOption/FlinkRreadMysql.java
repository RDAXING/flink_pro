package mysqlOption;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 自定义程序获取mysql中的数据
 */
public class FlinkRreadMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Map<String, Object>> mysqlSource = env.addSource(new MysqlSource(), "mysqlSource");
        mysqlSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class MysqlSource extends RichSourceFunction<Map<String,Object>>{
        private PreparedStatement ps=null;
        private Connection connection=null;
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        String username  = "root";
        String password = "";

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "select * from datataskinfor_bak";
//            String sql = "select dataTaskId,dataTaskName from datataskinfor_bak";
            ps = connection.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Map<String, Object>> sourceContext) throws Exception {
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while(resultSet.next()){
                HashMap<String, Object> map = new HashMap<>();
                for(int i=0;i<columnCount;i++){
                    String columnName = metaData.getColumnName(i + 1);
                    Object field = resultSet.getObject(i + 1);
                    map.put(columnName,field);
                }
                sourceContext.collect(map);
            }
        }

        @Override
        public void cancel() {

        }

        @Override
        public void close() throws Exception {
            super.close();
            if(connection != null){
                connection.close();
            }
            if (ps != null){
                ps.close();
            }
        }

        //获取mysql连接配置
        public Connection getConnection(){
            try {
                //加载驱动
                Class.forName(driver);
                //创建连接
                connection = DriverManager.getConnection(url,username,password);
            } catch (Exception e) {
                System.out.println("get connection occur exception, msg = "+e.getMessage());
                e.printStackTrace();
            }
            return  connection;
        }


        public void getScanTable(){
            Scan scan = new Scan();

        }



    }



}
