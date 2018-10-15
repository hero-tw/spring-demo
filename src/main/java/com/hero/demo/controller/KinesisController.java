package com.hero.demo.controller;

import com.hero.demo.client.KafkaStubClient;
import com.hero.demo.client.KinesisClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;


@RestController
@RequestMapping("/kinesis")
public class KinesisController {
    private KinesisClient client;

    public KinesisController(KinesisClient client) {
        this.client = client;
    }

    @RequestMapping(method=RequestMethod.POST)
    public void sendKinesis(@RequestBody String message) throws Exception {
        client.send(message);
    }

    @RequestMapping(method=RequestMethod.GET)
    public Collection<String> getKinesis() {
        return client.getRecords(10);
    }
}
