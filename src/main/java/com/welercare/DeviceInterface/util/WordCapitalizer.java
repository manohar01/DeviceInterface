package com.welercare.DeviceInterface.util;


import org.apache.flink.api.common.functions.MapFunction;

public class WordCapitalizer implements MapFunction<String, String> {
    @Override
    public String map(String s) {
        return s.toUpperCase();
    }
}
