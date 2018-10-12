package com.hero.demo.controller;

import com.hero.demo.client.KafkaStubClient;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

/**
 * Created by play on 10/11/18.
 */
@RestController
@RequestMapping("/kafka")
public class KafkaController {
    private KafkaStubClient client;

    public KafkaController(KafkaStubClient client) {
        this.client = client;
    }

    @RequestMapping(method=RequestMethod.POST)
    public void post(@RequestBody String message) throws Exception {
        client.send(message);
    }

    @RequestMapping(method=RequestMethod.GET)
    public Collection<String> get() {
        return client.getRecords(10);
    }
}
