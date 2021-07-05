package customSource;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MySource {
    public static void main(String[] args) {
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        String userName = "root";
        String password = "";
        JDBCInputFormat jdbcConn = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverName)
                .setDBUrl(url)
                .setUsername(userName)
                .setPassword(password)
                .setQuery("select dataTaskId,dataTaskName from datataskinfor_bak")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO))
                .finish();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> inputds = env.createInput(jdbcConn);
        inputds.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


