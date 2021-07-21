package com.rdx.demo;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * 项目：flink_pro
 * 包名：com.rdx.demo
 * 作者：rdx
 * 日期：2021/7/21 16:43
 *
 */
public class PropertiesConstants {
    public static  String PROPERTIES_FILE_NAME = "/application-dev-location.properties";

    static {
        // 读取配置文件 application.properties 中的 ykc.profile
        getProp();
    }


    private static void getProp(){
        Properties prop = new Properties();
        InputStream in = PropertiesConstants.class.getClassLoader().getResourceAsStream("application.properties");
        try {

            InputStreamReader isr = new InputStreamReader(in, "UTF-8");
            prop.load(isr);
            in.close();
            isr.close();

            String type = prop.getProperty("ykc.profile");
            PROPERTIES_FILE_NAME = MessageFormat.format("/application_{0}.properties", type);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(PROPERTIES_FILE_NAME);
    }
}
