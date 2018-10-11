package com.hero.demo;

import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
//		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		SpringApplication.run(DemoApplication.class, args);
//	}
}
