package com.hero.demo.controller;

import com.hero.demo.client.KafkaStubClient;
import com.hero.demo.client.KinesisClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.Map;


@RestController
@RequestMapping("/kinesis")
public class KinesisController {
    private KinesisClient client;

    public KinesisController(KinesisClient client) {
        this.client = client;
    }

    @RequestMapping(method=RequestMethod.POST)
    public void sendKinesis(@RequestBody Map<String, Object> user) throws Exception {
        client.sendUser(
                (Integer) user.get("id"),
                (String) user.get("name"),
                (String) user.get("company"));
    }

    @RequestMapping(method=RequestMethod.GET)
    public Collection<Map<String, Object>> getKinesis() {
        Collection<Map<String, Object>> result = client.getUsers(10);

        return result;
    }
}
