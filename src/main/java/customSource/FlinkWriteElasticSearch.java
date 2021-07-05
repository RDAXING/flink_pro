//package customSource;
//
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class FlinkWriteElasticSearch {
//    public static void main(String[] args) {
//        String[] esIp = "121.52.212.147".split(",");
////        httpHosts.add(new HttpHost("121.52.212.147",9200,"http"));
//        Map<String, String> config = new HashMap<>();
//        config.put("cluster.name", "Apollo-CBD");
//// This instructs the sink to emit after every element, otherwise they would be buffered
//        config.put("bulk.flush.max.actions", "1");
//
//        for(int i = 0; i < esIp.length; i ++){
//            client.addTransportAddress(new InetSocketTransportAddress(esIp[i], "9300"));
//        }
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> dataSource = env.readTextFile("C:\\Users\\admin\\Desktop\\sparktest\\flinkdemo\\person.txt");
////        dataSource.addSink(new ElasticsearchSink<String>(
////                config,
////                transportAddresses,
////               new MyElasticSearchSink(columns)
////            )
////        );
//        dataSource.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
//            public IndexRequest createIndexRequest(String s) {
//                String colums = "name,age,gender,height";
//                String[] split = s.split(";");
//                String[] cols = colums.split(",");
//                HashMap<String, String> json = new HashMap<>();
//                for(int i=0;i<split.length;i++){
//                    String val = split[i];
//                    String col = cols[i];
//                    json.put(col,val);
//                }
//
//                return Requests.indexRequest()
//                        .index("rdx_per")
//                        .type("rdx_per")
//                        .source(json);
//            }
//
//            @Override
//            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                indexer.add(createIndexRequest(element));
//            }
//        }));
//
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
//
//class MyElasticSearchSink implements ElasticsearchSinkFunction<String>{
//    private String colums;
//
//    public MyElasticSearchSink(String colums) {
//        this.colums = colums;
//    }
//
//    @Override
//    public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
//        String[] split = s.split(";");
//        String[] cols = colums.split(",");
//        HashMap<String, String> json = new HashMap<>();
//        for(int i=0;i<split.length;i++){
//            String val = split[i];
//            String col = cols[i];
//            json.put(col,val);
//        }
//        IndexRequest source = Requests.indexRequest().index("rdx_per").type("rdx_per").source(json);
//        requestIndexer.add(source);
//    }
//}
