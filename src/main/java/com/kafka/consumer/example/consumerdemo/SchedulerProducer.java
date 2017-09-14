package com.kafka.consumer.example.consumerdemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Component
@EnableScheduling
public class SchedulerProducer {
    private Properties props = new Properties();
    private KafkaProducer producer;

    @Value("${bootstrap.servers}")
    private String bootStrapServer;

    @Value("${client.id:test}")
    private String clientId;

    @Value("${enable.auto.commit:true}")
    private Boolean enableAutoCommit;

    @Value("${auto.commit.interval.ms:1000}")
    private Integer autoCommitIntervalMs;

    @Value("${session.timeout.ms:30000}")
    private Integer sessionTimeoutMs;

    @Value("${key.serializer}")
    private String keyDeserializer;

    @Value("${value.serializer}")
    private String valueDeserializer;

    @PostConstruct
    public void createProducer() {
        props.put("bootstrap.servers", bootStrapServer);
        props.put("client.id", clientId);
        props.put("key.serializer", keyDeserializer);
        props.put("value.serializer", valueDeserializer);
        producer = new KafkaProducer(props);
    }

    private int serial = 0;

    @Scheduled(fixedDelay = 5000)
    public void generateMessage() {
        producer.send(new ProducerRecord("test1", "test1-key" + (serial), "test1-value" + serial));
        producer.send(new ProducerRecord("test1", "test2-key" + (serial), "test2-value" + serial));
        ++serial;
    }
}
