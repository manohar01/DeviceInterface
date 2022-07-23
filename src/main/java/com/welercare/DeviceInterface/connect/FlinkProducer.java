package com.welercare.DeviceInterface.connect;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class FlinkProducer {
    public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer011<String>(kafkaAddress, topic, new SimpleStringSchema());
    }
}
