package com.welercare.DeviceInterface.controller;

import com.welercare.DeviceInterface.service.FlinkKafkaConsumer123;
import com.welercare.DeviceInterface.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeviceController {

    @Autowired
    KafkaProducerService kafkaProducerService;

    @Autowired
    FlinkKafkaConsumer123 flinkKafkaConsumer;

    // Publish messages using the GetMapping
    @GetMapping("/publish/{message}")
    public String publishMessage(@PathVariable("message") final String message) throws Exception {

        // Sending the message
        kafkaProducerService.sendMessage(message);
        flinkKafkaConsumer.capitalize();

        return "Published Successfully";
    }

}
