package com.clescot.crashtest;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CrashTestSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(CrashTestSourceConnector.class.getName());
    public static final String CONNECTOR_CRASH_UNIT_KEY = "connector.crash.unit";
    public static final String CONNECTOR_CRASH_INITIAL_DELAY_KEY = "connector.crash.initial.delay";
    public static final String CONNECTOR_CRASH_PERIOD_KEY = "connector.crash.period";
    private final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> scheduledFuture;
    public static final String TASK_CONFIG_PREFIX = "task.";
    private Version version;
    private Map<String, String> configuration;
    private volatile boolean mustThrowError;

    public CrashTestSourceConnector() {
        version = new Version();
    }

    @Override
    public void start(Map<String, String> configuration) {
            this.configuration = configuration;
        Preconditions.checkArgument(configuration.containsKey(CONNECTOR_CRASH_UNIT_KEY));
        Preconditions.checkArgument(configuration.containsKey(CONNECTOR_CRASH_INITIAL_DELAY_KEY));
        Preconditions.checkArgument(configuration.containsKey(CONNECTOR_CRASH_PERIOD_KEY));
        Preconditions.checkState(scheduledFuture==null,"'start' method has already been called");
        long initialDelay = Long.parseLong(configuration.get(CONNECTOR_CRASH_INITIAL_DELAY_KEY));
        logger.debug("initial delay:{}",initialDelay);
        long period = Long.parseLong(configuration.get(CONNECTOR_CRASH_PERIOD_KEY));
        logger.debug("period:{}",period);
        TimeUnit unit = TimeUnit.valueOf(configuration.get(CONNECTOR_CRASH_UNIT_KEY));
        logger.debug("unit:{}",unit);
        scheduledFuture = SCHEDULER.scheduleAtFixedRate(() -> {logger.error("error scheduled");mustThrowError=true;context.raiseError(new RuntimeException("planned connector failure"));}, initialDelay, period, unit);

    }

    @Override
    public Class<? extends Task> taskClass() {
        return CrashTestSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Preconditions.checkArgument(maxTasks>0,"maxTasks must be positive");
        Map<String, String> taskConfig = configuration.entrySet().stream().filter(entry -> entry.getKey().startsWith(TASK_CONFIG_PREFIX)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        List<Map<String,String>> taskConfigs = Lists.newArrayList();
        for (int j = 0; j < maxTasks; j++) {
            taskConfigs.add(taskConfig);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        config.define(CONNECTOR_CRASH_UNIT_KEY, ConfigDef.Type.STRING,"SECONDS", ConfigDef.Importance.HIGH,"time unit of the delay for the connector to crash")
              .define(CONNECTOR_CRASH_INITIAL_DELAY_KEY, ConfigDef.Type.STRING,"60", ConfigDef.Importance.HIGH,"delay for the connector before first crash")
              .define(CONNECTOR_CRASH_PERIOD_KEY, ConfigDef.Type.STRING,"60", ConfigDef.Importance.HIGH,"period for the connector to crash");
        return config;
    }

    @Override
    public String version() {
        return version.getVersion();
    }

    public boolean isMustThrowError() {
        return mustThrowError;
    }
}
