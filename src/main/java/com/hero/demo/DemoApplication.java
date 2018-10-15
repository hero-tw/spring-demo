package com.hero.demo;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hero.demo.ApplicationContextAware.getContext;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);

		postInit();
	}

	private static void postInit() {
		System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

		Worker worker = getContext().getBean(Worker.class);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		executorService.submit(() -> {
					try {
						worker.run();
					} catch (Throwable t) {
						t.printStackTrace();
					}
				}
		);
	}
}
