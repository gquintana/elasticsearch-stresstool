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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.*;

public class LogStashJsonReporterTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int METRIC_NUMBER = 5;
    private static final int METRIC_ITERATION = 5;
    private static final long FILE_TIMEOUT = 5000L;

    private static void waitFileLines(File file, int nbLine, long timeoutMs) throws InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        int lineCount = 0;
        while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeoutMs) {
            if (file.exists()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lineCount++;
                        if (lineCount >= nbLine) {
                            return;
                        }
                    }
                } catch (IOException ioExc) {
                }
            }
            Thread.sleep(Math.max(timeoutMs / 10, 1000L));
        }
        stopwatch.stop();
        fail("Not enough lines in file " + lineCount + " after " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
        for (int i = 0; i < METRIC_ITERATION; i++) {
            for (int j = 0; j < METRIC_NUMBER; j++) {
                Timer.Context context = metricRegistry.timer(metricName + "." + j).time();
                Thread.sleep(90L + random.nextInt(20));
                context.stop();
            }
        }
        waitFileLines(jsonFile, METRIC_NUMBER * 2, FILE_TIMEOUT);
        jsonReporter.close();
        // Then
        try (FileInputStream jsonIS = new FileInputStream(jsonFile)) {
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(jsonIS).map();
            assertThat((String) map.get("name"), startsWith(metricName));
            assertEquals("timer", map.get("type"));
            int count = (Integer) map.get("count");
            assertTrue(count >1 && count <= METRIC_ITERATION);
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
        for (int i = 0; i < METRIC_ITERATION; i++) {
            for (int j = 0; j < METRIC_NUMBER; j++) {
                metricRegistry.counter(metricName + "." + j).inc();
            }
            Thread.sleep(90L + random.nextInt(20));
        }
        waitFileLines(jsonFile, METRIC_NUMBER * 2, FILE_TIMEOUT);
        jsonReporter.close();
        // Then
        try (FileInputStream jsonIS = new FileInputStream(jsonFile)) {
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(jsonIS).map();
            assertThat((String) map.get("name"), startsWith(metricName));
            assertEquals("counter", map.get("type"));
            int count = (Integer) map.get("count");
            assertTrue(count >1 && count <= METRIC_ITERATION);
        }
    }

}