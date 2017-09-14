package com.kafka.consumer.example.consumerdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/demo")
public class ConsumerController {

    private ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/consume")
    public String consume() throws JsonProcessingException {
        return consumerService.consume();
    }
}
