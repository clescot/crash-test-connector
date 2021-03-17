package com.clescot.crashtest;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CrashTestSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(CrashTestSourceTask.class.getName());
    private final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);
    private Version version;
    private  ScheduledFuture<?> scheduledFuture;
    public CrashTestSourceTask() {
        version = new Version();
    }
    private volatile boolean mustThrowError;
    public static final String TASK_CRASH_UNIT_KEY = "task.crash.unit";
    public static final String TASK_CRASH_INITIAL_DELAY_KEY = "task.crash.initial.delay";
    public static final String TASK_CRASH_PERIOD_KEY = "task.crash.period";


    @Override
    public String version() {
        return version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        Preconditions.checkArgument(props.containsKey(TASK_CRASH_UNIT_KEY));
        Preconditions.checkArgument(props.containsKey(TASK_CRASH_INITIAL_DELAY_KEY));
        Preconditions.checkArgument(props.containsKey(TASK_CRASH_PERIOD_KEY));
        Preconditions.checkState(scheduledFuture==null,"'start' method has already been called");
        long initialDelay = Long.parseLong(props.get(TASK_CRASH_INITIAL_DELAY_KEY));
        logger.debug("initial delay:{}",initialDelay);
        long period = Long.parseLong(props.get(TASK_CRASH_PERIOD_KEY));
        logger.debug("period:{}",period);
        TimeUnit unit = TimeUnit.valueOf(props.get(TASK_CRASH_UNIT_KEY));
        logger.debug("unit:{}",unit);
        scheduledFuture = SCHEDULER.scheduleAtFixedRate(() -> {logger.error("scheduled");mustThrowError = true;}, initialDelay, period, unit);

    }

    @Override
    public List<SourceRecord> poll() {
        if(mustThrowError){
            logger.error("exception");
            mustThrowError=false;
            throw new RuntimeException("failure");
        }
        return Lists.newArrayList();
    }

    @Override
    public void stop() {
        Preconditions.checkState(!SCHEDULER.isShutdown(),"'stop' method has already been called: scheduler is already shutting down ");
        SCHEDULER.shutdown();
    }
}
