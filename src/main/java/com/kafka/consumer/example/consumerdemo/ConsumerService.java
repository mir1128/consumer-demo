package com.kafka.consumer.example.consumerdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Component
public class ConsumerService {
    private Properties props = new Properties();
    private KafkaConsumer consumer;

    @Value("${bootstrap.servers}")
    private String bootStrapServer;

    @Value("${group.id:test}")
    private String groupId;

    @Value("${enable.auto.commit:true}")
    private Boolean enableAutoCommit;

    @Value("${auto.commit.interval.ms:1000}")
    private Integer autoCommitIntervalMs;

    @Value("${session.timeout.ms:30000}")
    private Integer sessionTimeoutMs;

    @Value("${key.deserializer}")
    private String keyDeserializer;

    @Value("${value.deserializer}")
    private String valueDeserializer;

    @PostConstruct
    public void createConsumer() {
        props.put("bootstrap.servers", bootStrapServer);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalMs);
        props.put("session.timeout.ms", sessionTimeoutMs);
        props.put("key.deserializer", keyDeserializer);
        props.put("value.deserializer", valueDeserializer);
        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("test1" , "test2"));
    }

    public String consume() throws JsonProcessingException {
        ConsumerRecords<String, String> records = consumer.poll(100);

        Map<String, String> result = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            result.put(record.key(), record.value());
        }
        return new ObjectMapper().writeValueAsString(result);
    }
}
