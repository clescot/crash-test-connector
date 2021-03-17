package com.clescot.crashtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {

    private static final String VERSION_PROPERTIES = "version.properties";
    private static final Logger logger = LoggerFactory.getLogger(Version.class.getName());
    private static final Object VERSION_KEY = "project.version";

    public String getVersion() {
        Properties properties = new Properties();
        String version;
        InputStream versionInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(VERSION_PROPERTIES);

        try {
            properties.load(versionInputStream);
        } catch (IOException e) {
            logger.error("cannot read version.properties", e);
            throw new RuntimeException(e);
        }
        version = (String) properties.get(VERSION_KEY);
        return version;
    }
}
