package com.clescot.crashtest;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static com.clescot.crashtest.CrashTestSourceConnector.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class CrashTestSourceConnectorTest {

    private CrashTestSourceConnector crashTestSourceConnector;

    @BeforeEach
    public void setup() {
        crashTestSourceConnector = new CrashTestSourceConnector();
    }

    @Test
    void test_start_empty_map() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> crashTestSourceConnector.start(Maps.newHashMap()));
    }

    @Test
    void test_start_map_missing_CONNECTOR_CRASH_UNIT_KEY() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HashMap<String, String> configuration = Maps.newHashMap();
            configuration.put(CONNECTOR_CRASH_INITIAL_DELAY_KEY, "60");
            configuration.put(CONNECTOR_CRASH_PERIOD_KEY, "60");
            crashTestSourceConnector.start(configuration);
        });
    }

    @Test
    void test_start_map_missing_CONNECTOR_CRASH_INITIAL_DELAY_KEY() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HashMap<String, String> configuration = Maps.newHashMap();
            configuration.put(CONNECTOR_CRASH_UNIT_KEY, "SECONDS");
            configuration.put(CONNECTOR_CRASH_PERIOD_KEY, "60");
            crashTestSourceConnector.start(configuration);
        });
    }

    @Test
    void test_start_map_missing_CONNECTOR_CRASH_PERIOD_KEY() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HashMap<String, String> configuration = Maps.newHashMap();
            configuration.put(CONNECTOR_CRASH_UNIT_KEY, "SECONDS");
            configuration.put(CONNECTOR_CRASH_INITIAL_DELAY_KEY, "60");
            crashTestSourceConnector.start(configuration);
        });
    }

    @Test
    void test_start_nominal_case() {
        HashMap<String, String> configuration = Maps.newHashMap();
        configuration.put(CONNECTOR_CRASH_UNIT_KEY, "SECONDS");
        configuration.put(CONNECTOR_CRASH_INITIAL_DELAY_KEY, "60");
        configuration.put(CONNECTOR_CRASH_PERIOD_KEY, "60");
        crashTestSourceConnector.start(configuration);
    }

    @Test
    void test_crash() {
        HashMap<String, String> configuration = Maps.newHashMap();
        configuration.put(CONNECTOR_CRASH_UNIT_KEY, "SECONDS");
        configuration.put(CONNECTOR_CRASH_INITIAL_DELAY_KEY, "1");
        configuration.put(CONNECTOR_CRASH_PERIOD_KEY, "1");
        crashTestSourceConnector.start(configuration);
        await().atMost(5, SECONDS).until(()->crashTestSourceConnector.isMustThrowError());
    }
}