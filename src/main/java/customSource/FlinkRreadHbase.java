package customSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * flink消费kafka中的数据
 */
public class FlinkRreadHbase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Map<String, String>> hbaseDataSource = env.addSource(new MyReadHbase("TEST:SHIXIN", "", "", "", ""));
        hbaseDataSource.print();
        try {
            env.execute("hbaseJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MyReadHbase extends RichSourceFunction<Map<String,String>>{
    private String tableName;
    private String rowkeyRegex;
    private String columns;
    private String filter;
    private String interval;
    private static Connection conn =null;
    private static Table table = null;

    public MyReadHbase(String tableName, String rowkeyRegex, String columns, String filter, String interval) {
        this.tableName = tableName;
        this.rowkeyRegex = rowkeyRegex;
        this.columns = columns;
        this.filter = filter;
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh1.fahaicc.com,cdh2.fahaicc.com,cdh3.fahaicc.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, String.valueOf((1 << 30)));//1g
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        Scan scan = new Scan();
        FilterList filters = getScanOfMoreFilter(rowkeyRegex, columns, filter, interval, scan);
        if(filters.size() > 0){
            scan.setFilter(filters);
        }
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        Map<String, String> map = null;
        while(it.hasNext()){
            map = new HashMap<>();
            Result result = it.next();
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                map.put(columnName,value);
            }
            sourceContext.collect(map);

        }
    }

    @Override
    public void cancel() {
        try {
            if(table != null){
                table.close();
            }
            if(conn != null){
                conn.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * tableName=TEST:AMR_RDX&rowkeyRegex=^f[ef]_.+$&columns=ETL:status,ETL:createTime,ETL:title&interval=greated_or_equal:ETL:createTime:1582528003502,less_or_equal:ETL:createTime:1582638734926
     * 通过扫描过滤条件，
     * 1.可以根据rowkey进行过滤
     * 2.可以输出指定列columns
     * 3.可以输出指定的过滤条件filter
     * 4.可以进行区间过滤 例如createTime
     * @param
     * @param rowkeyRegex hbase的rowkey的正则 例：^\\d[0a]_.+$
     * @param columns 指定要获取的列 例：family:colum 多个过滤条件用‘,’分割 ----> ETL:name
     * @param filter 过滤的条件  例： family:colum:value 多个过滤条件用‘,’分割 ----> ETL:name:zhangsan
     * @param interval 区间过滤的条件  例： greated_or_equal:ETL:createTime:1582528003502,less_or_equal:ETL:createTime:1582638734926 多个过滤条件用‘,’分割
     */
    public static FilterList getScanOfMoreFilter(String rowkeyRegex, String columns, String filter, String interval, Scan scan){
//		new RegexStringComparator("")
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//		Scan scan = new Scan();
        ResultScanner scanner = null;
        if(StringUtils.isNotBlank(rowkeyRegex)){
            RegexStringComparator reg = new RegexStringComparator(rowkeyRegex);
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, reg);
            filterList.addFilter(rowFilter);
        }
        if(StringUtils.isNoneBlank(filter)){
            String[] split = filter.split(",");
            for(String term : split){
                String trim = StringUtils.trim(term);
                Pattern p = Pattern.compile("(.+):(.+):(.+)");
                Matcher m = p.matcher(trim);
                SingleColumnValueFilter scv = null;
                if (m.find() && m.groupCount() == 3) {
                    String family = m.group(1);
                    String column = m.group(2);
                    String value = m.group(3);
                    scv = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                            CompareOperator.EQUAL, Bytes.toBytes(value));
                    if ("null".equalsIgnoreCase(value) || "".equalsIgnoreCase(value)) {
                        // 如果为false，当这一列不存在时，会返回所有的列信息
                        scv.setFilterIfMissing(false);
                    } else {
                        // 如果为true，当这一列不存在时，不会返回
                        scv.setFilterIfMissing(true);
                    }
                    if(scan != null) {
                        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
                    }
                }
                if(scv != null){
                    filterList.addFilter(scv);
                }
            }
        }

        if(StringUtils.isNotBlank(interval)){
            String[] split = interval.split(",");
            for(String term : split){
                SingleColumnValueFilter scvf = getInterval(term);
                if(null != scvf){
                    filterList.addFilter(scvf);
                }
            }
        }

        if(StringUtils.isNoneBlank(columns)){
            String[] split = columns.split(",");
            for(String term : split){
                Pattern p = Pattern.compile("(.+):(.+)");
                Matcher m = p.matcher(StringUtils.trim(term));
                if (m.find() && m.groupCount() == 2) {
                    String family = m.group(1);
                    String column = m.group(2);
                    scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
                }
            }
        }

        return filterList;
    }

    /**
     * 区间匹配 返回过滤条件SingleColumnValueFilter
     * @param term 案例：greated_or_equal:ETL:createTime:1582528003502,less_or_equal:ETL:createTime:1582638734926 多个参数用“,”进行分割
     * @return
     */
    public static SingleColumnValueFilter getInterval(String term){

        Pattern p = Pattern.compile("(.+):(.+):(.+):(.+)");
        Matcher m = p.matcher(StringUtils.trim(term));
        SingleColumnValueFilter scvf = null;
        if (m.find() && m.groupCount() == 4) {
            String flag = m.group(1);
            String family = m.group(2);
            String column = m.group(3);
            String value = m.group(4);
            switch (flag) {
                case "less":
                    scvf = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                            CompareOperator.LESS, Bytes.toBytes(value));
                    return scvf;
                case "greated":
                    scvf = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                            CompareOperator.GREATER, Bytes.toBytes(value));
                    return scvf;
                case "greated_or_equal":
                    scvf = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                            CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(value));
                    return scvf;
                case "less_or_equal":
                    scvf = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                            CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(value));
                    return scvf;
                case "not_equal":
                    scvf = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                            CompareOperator.NOT_EQUAL, Bytes.toBytes(value));
                    return scvf;
                default:
                    return null;

            }
        }
        return null;
    }

}
