package customSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import sun.security.provider.MD5;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class FlinkWriteHbase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.readTextFile("C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\person.txt");
        dataSource.addSink(new MyHbaseSink("TEST:PERSON","FK","name,age,gender,height"));
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MyHbaseSink extends RichSinkFunction<String>{
    private final static String[] hexDigits = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};
    private Connection conn = null;
    private Table table = null;
    private String tableName ;
    private String family ;
    private String columns;

    private List<Put> puts = new ArrayList<>();
    public MyHbaseSink(String tableName, String family, String columns) {
        this.tableName = tableName;
        this.family = family;
        this.columns = columns;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getHbaseConn();
        table = conn.getTable(TableName.valueOf(tableName));


    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] split = value.split(";");
        String[] cols = columns.split(",");
        Put put = new Put(Bytes.toBytes(MD5Encode2(split[0])));

        for(int i=0;i<split.length;i++) {
            String val = split[i];
            String col = cols[i];
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(col),Bytes.toBytes(val));
        }

        puts.add(put);

        if(puts.size()>500){
            table.put(puts);
            puts = new ArrayList<>();
        }

    }


    @Override
    public void close() throws Exception {
        if(!puts.isEmpty()){
            table.put(puts);
        }else{

            if(table != null){
                table.close();
            }
            if(conn!= null){
                conn.close();

            }
        }

    }

    public void getHbaseConn(){
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh1.fahaicc.com,cdh2.fahaicc.com,cdh3.fahaicc.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, String.valueOf((1 << 30)));//1g
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * MD5编码
     * @param origin 原始字符串
     * @return 经过MD5加密之后的结果
     */
    public static String MD5Encode2(String origin) {
        String resultString = null;
        try {
            resultString = origin;
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(resultString.getBytes("UTF-8"));
            resultString = byteArrayToHexString(md.digest());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultString;
    }

    private static String byteArrayToHexString(byte[] b) {
        StringBuffer resultSb = new StringBuffer();
        for (int i = 0; i < b.length; i++) {
            resultSb.append(byteToHexString(b[i]));
        }
        return resultSb.toString();
    }

    private static String byteToHexString(byte b) {
        int n = b;
        if (n < 0) n = 256 + n;
        int d1 = n / 16;
        int d2 = n % 16;
        return hexDigits[d1] + hexDigits[d2];
    }
}