package com.clescot.crashtest;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

import static com.clescot.crashtest.CrashTestSourceTask.*;
import static org.junit.jupiter.api.Assertions.*;

class CrashTestSourceTaskTest {

    private CrashTestSourceTask task;


    @BeforeEach
    public void setup(){
        task = new CrashTestSourceTask();

    }
    @Test
    void poll() {
        HashMap<String, String> config = Maps.newHashMap();
        config.put(CRASH_UNIT_KEY,"SECONDS");
        config.put(CRASH_INITIAL_DELAY_KEY,"1");
        config.put(CRASH_PERIOD_KEY,"1");
        long now = System.currentTimeMillis();
        task.start(config);
        assertThrows(RuntimeException.class,()->  {
            do {
                task.poll();
            }while (System.currentTimeMillis()<=now+5000);

        });

    }
}