package demo_f;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 获取文件数据并存放至elasticsearch
 */
public class WriteDataToEs {
    private final static String[] hexDigits = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};
    public static Logger logger = Logger.getLogger(WriteDataToEs.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.readTextFile("C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\person.txt");
        dataSource.print();

        //将数据写入到指定elasticsearch中的索引中
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("121.52.212.147",9200));

        dataSource.addSink(new ElasticsearchSink.Builder<String>(httpHosts,new MySinkElasticSearch()).build());
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义写入elasticsearch中的逻辑
     */
    public static class MySinkElasticSearch implements ElasticsearchSinkFunction<String>{


        @Override
        public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
//            Map<String, String> mapping = new HashMap<>();
//            mapping.put("es.mapping.id", "age");
////				mapping.put("es.mapping.id", "pname");
//            //更新数据配置
//            mapping.put("es.write.operation", "upsert");

            //id设置

            HashMap<String, String> map = new HashMap<>();
            map.put("name",s.split(";")[0]);
            map.put("age",s.split(";")[1]);
            map.put("sex",s.split(";")[2]);
            map.put("weight",s.split(";")[3]);
            IndexRequest source = Requests.indexRequest()
                    .index("rdx_pfk")
                    .type("rdx_pfk")
                    /*自定义文档id，如何不设置id,则es会自动生成一个默认的id*/
                    .id(MD5Encode2(s.split(";")[0]))//自定义文档id
                    .source(map);

            requestIndexer.add(source);
        }
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
            logger.error(e);
        }
        return resultString;
    }
}
