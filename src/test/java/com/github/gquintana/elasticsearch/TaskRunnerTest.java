package com.github.gquintana.elasticsearch;

import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TaskRunnerTest {
    public static class TestTask extends Task {
        private final AtomicInteger prepareCounter = new AtomicInteger();
        private final AtomicInteger executeCounter = new AtomicInteger();
        public TestTask() {
            super(null, null);
        }

        @Override
        public void prepare() {
            prepareCounter.incrementAndGet();
        }

        @Override
        public void execute() {
            executeCounter.incrementAndGet();
        }
        public int getExecuteCounter() {
            return executeCounter.get();
        }

        public int getPrepareCounter() {
            return prepareCounter.get();
        }
    }
    @Test
    public void testRun() throws Exception {
        // Given
        MetricRegistry metricRegistry = new MetricRegistry();
        TaskRunner taskRunner = new TaskRunner(metricRegistry);
        taskRunner.setThreadNumber(4);
        taskRunner.setIterationNumber(10);
        TestTask testTask = new TestTask();
        // When
        taskRunner.start();
        taskRunner.run(testTask).get();
        taskRunner.stop();
        // Then
        assertEquals(1, testTask.getPrepareCounter());
        assertEquals(40, testTask.getExecuteCounter());
        assertEquals(40, metricRegistry.getTimers().get(testTask.getClass().getName()+".timer").getCount());
    }
}