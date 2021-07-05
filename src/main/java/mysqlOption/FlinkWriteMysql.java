package mysqlOption;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FlinkWriteMysql {
    private static String driverName = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    private static String userName = "root";
    private static String password = "";
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\person.txt");
        DataStreamSink<String> resSink = source.addSink(JdbcSink.sink(
                "replace into person_message(name, age, gender, height) values(?,?,?,?)",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        String[] split = s.split(";");
                        preparedStatement.setString(1, split[0]);
                        preparedStatement.setString(2, split[1]);
                        preparedStatement.setString(3, split[2]);
                        preparedStatement.setString(4, split[3]);
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driverName).withUrl(url).withUsername(userName).withPassword(password).build()));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
