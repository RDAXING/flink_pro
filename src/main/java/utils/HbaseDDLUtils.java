package utils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size.Unit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.junit.Test;


public class HbaseDDLUtils {
    private static Logger logger = Logger.getLogger(HbaseDDLUtils.class);
    static Connection CONN;
    private static Map<String, Table> TABLES;
    private static Map<String, Admin> ADMINS;
    //小region，直接合并，默认100M 单位：mb
    private static double lower_size = 100;
    //大region，按条件合并，默认5G 单位：mb
    private static double upper_size = 5120;
    //最大region大小，建议和region split的阀值大小相同，默认10G 单位：mb
    private static double region_max_size = 10240;
    //最少region个数，默认10个
    private static int least_region_count = 10;

    static{
        init();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            long time = System.currentTimeMillis();
            closeAllTable_Admin();
            logger.info("hbase table 关闭完成");
            if (CONN != null) {
                try {
                    CONN.close();
                } catch (IOException e) {
                }
            }
            logger.info("hbase conn 关闭完成");
            logger.info("hbase资源关闭完成,用时:" + (System.currentTimeMillis() - time));
        }));
    }


    public static void init(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh1.fahaicc.com,cdh2.fahaicc.com,cdh3.fahaicc.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, String.valueOf((1 << 30)));//1g
        try {
            CONN = ConnectionFactory.createConnection(conf);
            ADMINS = new HashMap<String,Admin>();
            TABLES = new HashMap<String,Table>();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 关闭所有的table连接
     */
    private static void closeAllTable_Admin() {
        if (TABLES != null && !TABLES.isEmpty()) {
            TABLES.values().forEach(t -> {
                try {
                    t.close();
                    logger.info("table is close!!!");
                } catch (IOException e) {
                }
            });
            TABLES.clear();
        }

        if(ADMINS != null && !ADMINS.isEmpty()){
            ADMINS.values().forEach(a ->{
                try {
                    a.close();
                    logger.info("admin is close!!!");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            ADMINS.clear();
        }
    }


    public static Admin getAdmin(String tableName){
        Admin admin = ADMINS.get(tableName);
        if(admin == null){
            synchronized (HbaseDDLUtils.class) {
                try {
                    admin = CONN.getAdmin();
                    ADMINS.put(tableName, admin);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        return admin;

    }

    public static Table getTable(String tableName){
        Table table = TABLES.get(tableName);
        if(table == null){
            synchronized (HbaseDDLUtils.class) {
                if(table == null){
                    try {
                        table = CONN.getTable(TableName.valueOf(tableName));
                        TABLES.put(tableName, table);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
        return table;

    }

    /**
     * 插入单条数据
     * @param tableName
     * @param put
     */
    public static void saveDataOne(String tableName,Put put){
        try {
            Table table = getTable(tableName);
            table.put(put);
            System.out.println("插入数据成功");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 获取单条数据
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result getData(String tableName ,String rowkey){
        Result result = null;
        Table table = getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setCacheBlocks(false);
        try {
            result = table.get(get);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;

    }

    /**
     * 根据rowkeys获取指定的数据
     * @param tableName 表名
     * @param family 列族
     * @param rowkeys hbase中的rowkey
     * @param columns 指定获取哪些列
     */
    public static Result[] getDatas(String tableName,List<String> rowkeys,String family ,String columns){
        List<Get> gets = new ArrayList<Get>();
        Result[] results = null;
        for(String rowkey : rowkeys){
            Get get = new Get(Bytes.toBytes(rowkey));
            if(StringUtils.isNoneBlank(columns)){
                String[] cols = columns.split(",");
                for(String col : cols){
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                }
            }
//			get.setCacheBlocks(false)
            gets.add(get);
        }

        Table table = HbaseDDLUtils.getTable(tableName);
        try {
            results = table.get(gets);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return results;
    }


    /**
     * 获取指定区间的rowkey
     * @param tableName
     * @param startRow 开始的rowkey
     * @param endRow 结束的rowkey
     * @return
     */
    public static ResultScanner getScanStartToEndRow(String tableName,String startRow,String endRow){
        Table table = getTable(tableName);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//		Iterator<Result> it = scanner.iterator();
//		while(it.hasNext()){
//			Result next = it.next();
//			System.out.println(Bytes.toString(next.getRow()));
//		}
        return scanner;
    }

    /**
     * 删除指定的rowkey的数据
     * @param tableName
     * @param rowkey
     */
    public static void deleteRow(String tableName,String rowkey,String family,String columns){
        Table table = getTable(tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            if(StringUtils.isNoneBlank(columns)){
                for(String col : columns.split(",")){
                    delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                }
            }
            table.delete(delete);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    /**
     * 构建 SingleColumnValueFilter
     *
     * @param argMap
     *            参数Map，key=singleColumnValue, value格式：family:column:value
     * @return
     */
    public static SingleColumnValueFilter getSingleColumnValueFilter(HashMap<String, String> argMap, Scan scan) {
        String singleColumnValue = StringUtils.strip(argMap.get("singleColumnValue".toLowerCase()));
        // family:column:value
        Pattern p = Pattern.compile("(.+):(.+):(.+)");
        Matcher m = p.matcher(singleColumnValue);
        if (m.find() && m.groupCount() == 3) {
            String family = m.group(1);
            String column = m.group(2);
            String value = m.group(3);
            SingleColumnValueFilter scv = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
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
            return scv;
        }

        return null;
    }

    /**
     *
     * @param tableName hbase 中的表名
     * @param regexFields 需要通过正则进行过滤的某些字段 例如：ETL:name:^zhang ----->多个过滤条件用“,”进行分割
     * @return
     */
    public static ResultScanner getRegexScan(String tableName,String regexFields){
        Table table = HbaseDDLUtils.getTable(tableName);
        Scan scan = new Scan();
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
        for(String term : regexFields.split(",")){
            String[] fqv = term.split(":");
            RegexStringComparator rsc = new RegexStringComparator(fqv[2]);
            SingleColumnValueFilter scv = new SingleColumnValueFilter(Bytes.toBytes(fqv[0]), Bytes.toBytes(fqv[1]), CompareOperator.EQUAL, rsc);
            if(scv != null){
                filterList.addFilter(scv);
                scan.addColumn(Bytes.toBytes(fqv[0]), Bytes.toBytes(fqv[1]));
            }
        }
        if(filterList.size()>0){
            scan.setFilter(filterList);
        }
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return scanner;

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
    public static FilterList getScanOfMoreFilter(String rowkeyRegex,String columns,String filter,String interval,Scan scan){
//		new RegexStringComparator("")
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
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

        SingleColumnValueFilter scvf = null;
        Pattern p = Pattern.compile("(.+):(.+):(.+):(.+)");
        Matcher m = p.matcher(StringUtils.trim(term));
        if (m.find() && m.groupCount() == 4 ) {
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

    /**
     * 获取某个字段中是否包含指定的内容
     * 案例：ETL:title:浙江省
     */
    public static SingleColumnValueFilter getSubStringContent(String subString){
        SingleColumnValueFilter subFilter = null;
        if(StringUtils.isNotBlank(subString)){
            Pattern p = Pattern.compile("(.+):(.+):(.+)");
            Matcher m = p.matcher(StringUtils.trim(subString));
            if (m.find() && m.groupCount() == 3){
                String family = m.group(1);
                String column = m.group(2);
                String value = m.group(3);
                SubstringComparator sub = new SubstringComparator(value);
                subFilter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareOperator.EQUAL, sub);
            }
            return subFilter;
        }
        return null;
    }

    /**
     * region拆分还没有测试完成，谨慎使用
     * @param tableName
     * @throws IOException
     */
    public static void splitRegion4Table(String tableName) throws IOException{
//		Admin admin = HbaseDDLUtils.getAdmin(tableName);
//		TableName tableNames = TableName.valueOf(tableName);
//		Collection<ServerName> regionServers = admin.getRegionServers();
//	    Iterator<ServerName> its = regionServers.iterator();
//	    while(its.hasNext()){
//	    	ServerName serverName = its.next();
//	    	List<RegionMetrics> regionMetrics = admin.getRegionMetrics(serverName, tableNames);
//	    	for(RegionMetrics rm : regionMetrics){
//	    		double fileSize = rm.getStoreFileSize().get(Unit.MEGABYTE);
//	    		if(fileSize > 4096){
//	    			byte[] regionName = rm.getRegionName();
////	    			admin.splitRegionAsync(regionName,"");
//	    			admin.splitRegion(regionName);
//	    		}
//	    	}
//
//	    	//拆分后
//	    	regionMetrics = admin.getRegionMetrics(serverName, tableNames);
//	    	for(RegionMetrics rm : regionMetrics){
//	    		double fileSize = rm.getStoreFileSize().get(Unit.MEGABYTE);
//	    		System.out.println(rm.getRegionName() + "》》》》》" + fileSize);
//	    	}
//	    }
    }

	/*
	 * 阀值：lower_size：小region，直接合并，默认100M
	 *     upper_size：大region，按条件合并，默认5G
	 *     region_max_size：最大region大小，建议和region split的阀值大小相同，默认10G
	 *     least_region_count：最少region个数，默认10个
	 * 规则：（待合并的region定义为A,B两个region）
	 * 当前region大小小于upper_size：
	 * 如果A region为空，则将当前region赋值给A
	 * 如果A region不为空，则将当前region赋值给B，则直接调用api对A,B进行合并，成功之后清空A,B继续遍历
	 * 当前region大小大于upper_size：
	 * 如果A region为空，放弃处理该region，直接继续遍历
	 * 如果A region不为空，并且A的大小小于lower_size或者A+当前的大小小于region_max_size，则将当前region赋值给B，则直接调用api对A,B进行合并，成功之后清空A,B继续遍历
	 * @author taochy
	 *
	 *
	 */


    /**
     * 合并region
     * 注意：
     * 		如果想要递归进行region的合并可以将该方法的最后几行注释掉的代码开启即可
     * @param admin
     * @param tableName
     */
    public static void mergeRegion4Table(Admin admin, String tableName) {
        long currentTimeMillis = 0L;
        long currentTimeMillis2 = 0L;
        int merge_count = 0;
        TableName tableNames = TableName.valueOf(tableName);
        try {


            Collection<ServerName> regionServers = admin.getRegionServers();
            Iterator<ServerName> its = regionServers.iterator();
            while(its.hasNext()){
                ServerName serverName = its.next();
                List<RegionMetrics> regionMetrics = admin.getRegionMetrics(serverName, tableNames);
                int filecount = regionMetrics.size();
//		    	 if(filecount > least_region_count){
                byte[] regionA = null;
                double sizeA = 0;
                byte[] regionB = null;

                for (RegionMetrics region : regionMetrics) {
                    double fileSize = region.getStoreFileSize().get(Unit.MEGABYTE);
                    String regionName = region.getNameAsString();
                    if (fileSize != 0) {
                        if (fileSize < upper_size) {
                            if (regionA == null) {
                                List<RegionInfo> regionInfoList = admin.getRegions(tableNames);
                                regionA = region.getRegionName();
                                sizeA = fileSize;
                            } else {
                                currentTimeMillis = System.currentTimeMillis();
                                regionB = region.getRegionName();
                                byte[][] nameofRegionsToMerge = new byte[2][];
                                nameofRegionsToMerge[0] = regionA;
                                nameofRegionsToMerge[1] = regionB;
                                Future<Void> mergeRegionsAsync = admin.mergeRegionsAsync(nameofRegionsToMerge,
                                        true);
                                currentTimeMillis2 = System.currentTimeMillis();
                                logger.info(
                                        Bytes.toString(regionA) + " & " + Bytes.toString(regionB) + "merge cost "
                                                + (currentTimeMillis2 - currentTimeMillis)/1000 + " seconds!");
                                regionA = null;
                                regionB = null;
                                sizeA = 0;
                                merge_count++;
                            }
                        } else {
                            if (regionA == null) {
                                //如果A region为空，放弃处理该region，直接继续遍历
                                continue;
                            } else {
                                //如果A region不为空，并且A的大小小于lower_size或者A+当前的大小小于region_max_size，则将当前region赋值给B，则直接调用api对A,B进行合并，成功之后清空A,B继续遍历
                                if (sizeA < lower_size || (sizeA + fileSize) < region_max_size) {
                                    regionB = region.getRegionName();
                                    currentTimeMillis = System.currentTimeMillis();
                                    byte[][] nameofRegionsToMerge = new byte[2][];
                                    nameofRegionsToMerge[0] = regionA;
                                    nameofRegionsToMerge[1] = regionB;
                                    Future<Void> mergeRegionsAsync = admin.mergeRegionsAsync(nameofRegionsToMerge,
                                            true);
                                    currentTimeMillis2 = System.currentTimeMillis();
                                    logger.info(
                                            Bytes.toString(regionA) + " & " + Bytes.toString(regionB) + "merge cost "
                                                    + (currentTimeMillis2 - currentTimeMillis)/1000 + " seconds!");
                                    regionA = null;
                                    regionB = null;
                                    sizeA = 0;
                                    merge_count++;
                                }
                            }
                        }
                    }else{
                        logger.info(regionName + "is empty,ignore!");
                    }
                }
            }
//		     }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//		logger.info("merge run " + merge_count + " times,continue!");
//		 if(merge_count != 0){
//			 logger.info("merge run "+ merge_count +" times,continue!");
//		 //递归操作，直到不需要继续合并
//		 mergeRegion4Table(admin,tableName);
//		 }else {
//			 logger.info("no need to merge any more,finish!");
//		 }
    }


    /**
     * 获取hbase的region的相关信息
     * @param tableName
     * @throws IOException
     */
    public static void getReginMsg(String tableName) throws IOException{
        Admin admin = getAdmin(tableName);
        Collection<ServerName> regionServers = admin.getRegionServers();
        Iterator<ServerName> its = regionServers.iterator();
        TableName tableName1 = TableName.valueOf(tableName);
        while(its.hasNext()){
            ServerName serverName = its.next();
            List<RegionMetrics> regionMetrics = admin.getRegionMetrics(serverName, tableName1);
            for (RegionMetrics region:regionMetrics){
                String size = region.getStoreFileSize().toString();
                String memSize = region.getMemStoreSize().toString();
                String regionName = new String(region.getRegionName());
                float locality = region.getDataLocality();
                List<RegionInfo> regionInfoList = admin.getRegions(tableName1);
                for (RegionInfo regionInfo:regionInfoList) {
                    System.err.println("name:"+regionInfo.getRegionNameAsString());
                    System.err.println("ENCODED:"+new String(regionInfo.getEncodedNameAsBytes()));
                    System.out.println("STARTKEY:"+new String(regionInfo.getStartKey()));
                    System.out.println("ENDKEY:"+new String(regionInfo.getEndKey()));
                    System.out.println("RegionId:"+regionInfo.getRegionId());
                    String regionId = new String(regionInfo.getRegionName());
                }
            }
        }

    }



}