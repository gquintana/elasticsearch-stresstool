package com.github.gquintana.elasticsearch.metric;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogStashJsonReporterTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void reportTimer() throws Exception {
        // Given
        MetricRegistry metricRegistry = new MetricRegistry();
        File jsonFile = temporaryFolder.newFile("test.json");
        LogStashJsonReporter jsonReporter = LogStashJsonReporter.forRegistry(metricRegistry)
                .build(jsonFile);
        jsonReporter.start(1, TimeUnit.SECONDS);
        // When
        String metricName = getClass().getName();
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            Timer.Context context = metricRegistry.timer(metricName).time();
            Thread.sleep(90L + random.nextInt(20));
            context.stop();
        }
        jsonReporter.close();
        // Then
        try (FileInputStream jsonIS = new FileInputStream(jsonFile)) {
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(jsonIS).map();
            assertEquals(metricName, map.get("name"));
            assertEquals("timer", map.get("type"));
            int counter = (Integer) map.get("counter");
            assertTrue(counter >= 10 && counter <= 20);
        }
    }

    @Test
    public void reportCounter() throws Exception {
        // Given
        MetricRegistry metricRegistry = new MetricRegistry();
        File jsonFile = temporaryFolder.newFile("test.json");
        LogStashJsonReporter jsonReporter = LogStashJsonReporter.forRegistry(metricRegistry)
                .build(jsonFile);
        jsonReporter.start(1, TimeUnit.SECONDS);
        // When
        String metricName = getClass().getName();
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            metricRegistry.counter(metricName).inc();
        }
        jsonReporter.close();
        // Then
        try (FileInputStream jsonIS = new FileInputStream(jsonFile)) {
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(jsonIS).map();
            assertEquals(metricName, map.get("name"));
            assertEquals("timer", map.get("type"));
            int counter = (Integer) map.get("counter");
            assertTrue(counter >= 10 && counter <= 20);
        }
    }

}