package com.github.gquintana.elasticsearch.metric;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LogStashJsonReporterTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static void waitFileLines(File file, int nbLine, long timeoutMs) throws InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        int lineCount = 0;
        while(stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeoutMs) {
            if (file.exists()) {
                try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while((line = reader.readLine()) != null) {
                        lineCount ++;
                        if (lineCount >= nbLine) {
                            return;
                        }
                    }
                } catch (IOException ioExc) {}
            }
            Thread.sleep(Math.max(timeoutMs / 10, 1000L));
        }
        stopwatch.stop();
        fail("Not enough lines in file "+lineCount+" after "+stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
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
        waitFileLines(jsonFile, 10, 10000L);
        jsonReporter.close();
        // Then
        try (FileInputStream jsonIS = new FileInputStream(jsonFile)) {
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(jsonIS).map();
            assertEquals(metricName, map.get("name"));
            assertEquals("timer", map.get("type"));
            int counter = (Integer) map.get("count");
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
        waitFileLines(jsonFile, 10, 10000L);
        jsonReporter.close();
        // Then
        try (FileInputStream jsonIS = new FileInputStream(jsonFile)) {
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(jsonIS).map();
            assertEquals(metricName, map.get("name"));
            assertEquals("counter", map.get("type"));
            int counter = (Integer) map.get("count");
            assertTrue(counter >= 10 && counter <= 20);
        }
    }

}