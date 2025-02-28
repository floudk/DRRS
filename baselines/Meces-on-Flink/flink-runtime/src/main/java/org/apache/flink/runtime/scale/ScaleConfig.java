package org.apache.flink.runtime.scale;

import org.apache.flink.configuration.ConfigConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Constants for the scaling
 */
public class ScaleConfig {
    static Logger LOG = LoggerFactory.getLogger(ScaleConfig.class);

    static String SCALE_CONFIG_FILE = "scale-conf.properties";

    // ----------------- Network -----------------
    public final int SCALE_PORT;
    public final int HIERARCHICAL_BIN_NUM; // default setting in MECES

    public final int NETTY_CHUNK_NUM;
    public final int NETTY_CHUNK_ORDER;
    public final int CACHE_CAPACITY;

    public static ScaleConfig Instance = new ScaleConfig();

    // no instances
    private ScaleConfig() {
        final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

        if (configDir == null) {
            LOG.error(
                    "No configuration directory set in environment variable {}",
                    ConfigConstants.ENV_FLINK_CONF_DIR);
            throw new RuntimeException("No configuration directory set in environment variable "
                    + ConfigConstants.ENV_FLINK_CONF_DIR);
        }
        File propertiesConfig = new File(configDir, SCALE_CONFIG_FILE);
        if (propertiesConfig.exists() && propertiesConfig.isFile() && propertiesConfig.canRead()) {
            LOG.info("Loading configuration from {}", propertiesConfig);
            Properties properties = new Properties();
            try (InputStream input = new FileInputStream(propertiesConfig)) {
                properties.load(input);
            } catch (IOException e) {
                LOG.error("Could not load configuration from {}", propertiesConfig, e);
                throw new RuntimeException(e);
            }
            this.SCALE_PORT = Integer.parseInt(properties.getProperty("scale-port", "33425"));
            LOG.info("Successfully loaded scale-port: {}", SCALE_PORT);

            this.HIERARCHICAL_BIN_NUM = Integer.parseInt(properties.getProperty("hierarchical-bin-num", "32"));
            LOG.info("Successfully loaded hierarchical-bin-num: {}", HIERARCHICAL_BIN_NUM);

            this.NETTY_CHUNK_NUM = Integer.parseInt(properties.getProperty("netty.chunk-num", "5"));
            LOG.info("Successfully loaded netty-chunk-num: {}", NETTY_CHUNK_NUM);

            this.NETTY_CHUNK_ORDER = Integer.parseInt(properties.getProperty("netty.chunk-order", "11")); // 11 - 16MB, 9 - 4MB
            LOG.info("Successfully loaded netty-chunk-order: {}", NETTY_CHUNK_ORDER);

            this.CACHE_CAPACITY = Integer.parseInt(properties.getProperty("drrs.cache.capacity", "200"));
            LOG.info("Successfully loaded cache-capacity: {}", CACHE_CAPACITY);

        } else {
            LOG.error("Configuration file {} does not exist or is not readable", propertiesConfig);
            throw new RuntimeException("Configuration file " + propertiesConfig + " does not exist or is not readable");
        }

    }
}
