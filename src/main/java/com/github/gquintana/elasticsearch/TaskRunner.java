package com.github.gquintana.elasticsearch;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ListeningExecutorService;
import org.elasticsearch.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskRunner {
    private int threadNumber;
    private int iterationNumber;
    private ListeningExecutorService executorService;
    private final MetricRegistry metricRegistry;
    public TaskRunner(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    private class TaskRunnable implements Runnable {
        private final Task task;
        private final AtomicBoolean running = new AtomicBoolean();

        public TaskRunnable(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            running.set(true);
            Timer timer = metricRegistry.timer(task.getClass().getName() + ".timer");
            Counter failCounter = metricRegistry.counter(task.getClass().getName() + ".failure");
            for (int j = 0; j < iterationNumber || !running.get(); j++) {
                try {
                    Timer.Context context = timer.time();
                    task.execute();
                    context.stop();
                } catch (EsStressToolException internalExc) {
                    throw internalExc;
                } catch (Exception e) {
                    failCounter.inc();
                }
            }
            running.set(false);
        }

        public void stop() {
            running.set(false);
        }
    }

    public ListenableFuture<?> run(final Task task) {
        List<ListenableFuture<?>> taskFutures = new ArrayList<>(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            taskFutures.add(executorService.submit(new TaskRunnable(task)));
        }
        return Futures.allAsList(taskFutures);
    }

    public void start() {
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadNumber));
    }

    public void stop() {
        executorService.shutdownNow();
    }

    public int getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    public int getIterationNumber() {
        return iterationNumber;
    }

    public void setIterationNumber(int iterationNumber) {
        this.iterationNumber = iterationNumber;
    }
}
