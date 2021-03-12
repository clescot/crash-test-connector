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
    public static final int PERIOD_IN_SECONDS = 2;
    public static final int INITIAL_DELAY_IN_SECONDS = 1;
    private static final ScheduledExecutorService SCHEDULER =Executors.newScheduledThreadPool(1);
    private Version version;
    private  ScheduledFuture<?> scheduledFuture;
    public CrashTestSourceTask() {
        version = new Version();
    }
    private volatile boolean mustThrowError;
    public static final String CRASH_UNIT_KEY = "crash.unit";
    public static final String CRASH_INITIAL_DELAY_KEY = "crash.initial.delay";
    public static final String CRASH_PERIOD_KEY = "crash.period";


    @Override
    public String version() {
        return version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        Preconditions.checkArgument(props.containsKey(CRASH_UNIT_KEY));
        Preconditions.checkArgument(props.containsKey(CRASH_INITIAL_DELAY_KEY));
        Preconditions.checkArgument(props.containsKey(CRASH_PERIOD_KEY));
        long initialDelay = Long.parseLong(props.get(CRASH_INITIAL_DELAY_KEY));
        logger.debug("initial delay:{}",initialDelay);
        long period = Long.parseLong(props.get(CRASH_PERIOD_KEY));
        logger.debug("period:{}",period);
        TimeUnit unit = TimeUnit.valueOf(props.get(CRASH_UNIT_KEY));
        logger.debug("unit:{}",unit);
        scheduledFuture = SCHEDULER.scheduleAtFixedRate(() -> {logger.error("scheduled");mustThrowError = true;}, initialDelay, period, unit);

    }

    @Override
    public List<SourceRecord> poll() {
        //return dummy records in a controlled rate per TASK.
        //two use cases : rate per message, or per b    yte
//        logger.error("mustThrowError={}",mustThrowError);
        if(mustThrowError){
            logger.error("exception");
            mustThrowError=false;
            throw new RuntimeException("failure");
        }
        return Lists.newArrayList();
    }

    @Override
    public void stop() {

    }
}