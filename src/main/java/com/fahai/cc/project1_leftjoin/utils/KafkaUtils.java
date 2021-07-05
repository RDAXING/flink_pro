package com.fahai.cc.project1_leftjoin.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.util.List;
import java.util.Map;

public class KafkaUtils {

    public static void main(String[] args) {
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\user\\join\\A.txt";
        try {
            kafkaProducer(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void kafkaConsumer(){

    }


    /**
     * 获取文件中的数据推送到kafka
     */
    private static void kafkaProducer(String path) throws Exception {
        //读取文件数据

        String text = FileUtils.readFileUtf8(new File(path));
        Map<String,String> map = JSON.parseObject(text, Map.class);
        String data = map.get("data");
        List<Map> maps = JSON.parseArray(data, Map.class);


    }
}
