package mysqlOption;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 写入数据到mysql中
 */
public class FlinkSinkMysql {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = LocalStreamEnvironment.getExecutionEnvironment();
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\person.txt";
        DataStreamSource<String> dataSource = env.readTextFile(path, "UTF-8");

        dataSource.print();
        dataSource.addSink(new MySinkMysql()).setParallelism(1);
        try {
            env.execute("mysql sink Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MySinkMysql extends RichSinkFunction<String>{
    private String driverName = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    private String userName = "root";
    private String password = "";
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driverName);
        connection = DriverManager.getConnection(url, userName, password);
        String sql = "replace into person_message(name, age, gender, height) values(?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);

    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] split = value.split(";");

        preparedStatement.setString(1,split[0]);
        preparedStatement.setString(2,split[1]);
        preparedStatement.setString(3,split[2]);
        preparedStatement.setString(4,split[3]);
        preparedStatement.execute();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
    }
}
