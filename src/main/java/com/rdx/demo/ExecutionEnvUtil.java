package com.rdx.demo;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 项目：flink_pro
 * 包名：com.rdx.demo
 * 作者：rdx
 * 日期：2021/7/21 16:56
 * flink获取配置文件数据
 */
public class ExecutionEnvUtil {
    static final Logger log = LogManager.getLogger("ExecutionEnvUtil");

    /**
     * ParameterTool全局参数
     * mergeWith()的会覆盖前面的
     *
     * @param args
     * @return
     */
    public static ParameterTool createParameterTool(final String[] args) {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromArgs(args))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (Exception e) {
            log.error("获取ParameterTool全局参数异常");
        }
        return ParameterTool.fromSystemProperties();

    }

    /**
     * ParameterTool全局参数
     *
     * @return
     */
    public static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());

        } catch (Exception e) {
            log.error("获取ParameterTool全局参数异常");
        }
        return ParameterTool.fromSystemProperties();
    }
}

