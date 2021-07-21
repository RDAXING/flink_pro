package com.rdx.demo;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 项目：flink_pro
 * 包名：com.rdx.demo
 * 作者：rdx
 * 日期：2021/7/21 14:51
 * java 获取配置文件数据
 */
public class PropConfig {
    static final Logger logger = LogManager.getLogger("PropConfig");
    private final static PropConfig instance = new PropConfig();
    private static Properties pro = null;
    private static Date lastUpdateTime = null;
    private static String FILE_PATH = null;
    private static final String CONMMENT_FILE_NAME="application.properties";



    static{
        Properties prop = new Properties();
        InputStream in = PropConfig.class.getClassLoader().getResourceAsStream(CONMMENT_FILE_NAME);
        try {

            InputStreamReader isr = new InputStreamReader(in, "UTF-8");
            prop.load(isr);
            in.close();
            isr.close();

            String type = prop.getProperty("ykc.profile");
            FILE_PATH = MessageFormat.format("application_{0}.properties", type);
            init(FILE_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * 初始化配置文件
     */
    private synchronized static void init(String FILE_PATH) {
        if(pro == null) {
            pro = new Properties();
        }
        InputStream in = null;
        try {
            in = PropConfig.class.getClassLoader().getResourceAsStream(FILE_PATH);
            // in = new FileInputStream("db.properties");
            if(in == null) {
                URL url = Thread.currentThread().getContextClassLoader().getResource(FILE_PATH);
                in = url.openStream();
            }
            pro.load(in);
            lastUpdateTime = new Date();
        } catch (Exception e) {
            logger.error("load common.properties 文件异常",e);
        } finally {
            try {
                if(in != null) {
                    in.close();
                }
            } catch (Exception e) {
                logger.error(e);
            }
        }
    }

    public static PropConfig getInstance() {
        // 为了HBase协处理器重新加载后可以重读配置文件而增加
        if (lastUpdateTime == null || countMinutes(lastUpdateTime, new Date()) > 5) {
            init(FILE_PATH);
        }
        return instance;
    }
    /**
     * 统计分钟数
     *
     * @param begin
     * @param end
     * @return
     */
    public static float countMinutes(Date begin, Date end) {
        long seconds = (end.getTime() - begin.getTime()) / 1000;
        float minutes = seconds / 60;

        return minutes;
    }

    /**
     * 依据主键获取相应的值，默认值为 null
     *
     * @param key
     * @return
     */
    public String getValue(String key) {
        return pro.getProperty(key, null);
    }

    public String getValue(String key, String defaultValue) {
        return pro.getProperty(key, defaultValue);
    }

    public static String getValue(Properties p,String key) {
        return p.getProperty(key, null);
    }

    public static String getValue(Properties p,String key, String defaultValue) {
        return p.getProperty(key, defaultValue);
    }

    public static Properties loadPro(String path) {
        Properties p = new Properties();
        InputStream in = null;
        while(path.startsWith("/")) {
            path = path.substring(1);
        }
        try{
            in = PropConfig.class.getClassLoader().getResourceAsStream(path);
            if(in == null) {
                URL url = Thread.currentThread().getContextClassLoader().getResource(path);
                in = url.openStream();
            }
            p.load(in);
            return p;
        } catch (Exception e) {
            logger.error(e);
            return null;
        } finally {
            try {
                in.close();
            } catch (Exception e) {
                logger.error(e);
            }
        }

    }

    public static void main(String[] args) {
        String property = PropConfig.pro.getProperty("mysql.url");
        System.out.println(property);
    }
}
