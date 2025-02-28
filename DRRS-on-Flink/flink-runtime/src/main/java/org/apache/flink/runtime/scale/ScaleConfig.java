package org.apache.flink.runtime.scale;

import org.apache.flink.configuration.ConfigConstants;

import org.apache.flink.runtime.scale.schedule.subscale.InternalKeyScheduler;

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

    // ----------------- Reroute -----------------
    public final long REROUTE_BUFFER_TIMEOUT; // 1s
    public final long REROUTE_BUFFER_LOCAL_SIZE; // 512 items
    public final long REROUTE_BUFFER_REMOTE_SIZE; // 1MB = 1024 * 1024 bytes = 1048576 bytes
    // remote size will depend on the network buffer size

    public final int NETTY_CHUNK_NUM;
    public final int NETTY_CHUNK_ORDER;

    public final int CACHE_CAPACITY;

    public final boolean ENABLE_DR; // decoupling and reroute
    public final boolean ENABLE_SUBSCALE; // subscale

    // ----------------- Subscale Scheduling -----------------
    public final boolean ENABLE_SUBSCALE_SCHEDULING;
    public final double STATE_SAMPLE_RATE;
    public final InternalKeyScheduler SUBSCALE_INTERNAL_KEY_SCHEDULER;
    public final double MIGRATION_BUFFER_EMERGENCY_RATIO;
    public final double FAIRNESS_WEIGHT;


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

            this.REROUTE_BUFFER_TIMEOUT = Long.parseLong(properties.getProperty("reroute.buffer-timeout", "1000"));
            LOG.info("Successfully loaded reroute-buffer-timeout: {}", REROUTE_BUFFER_TIMEOUT);

            this.REROUTE_BUFFER_LOCAL_SIZE = Long.parseLong(properties.getProperty("reroute.buffer-local-size", "512"));
            LOG.info("Successfully loaded reroute-buffer-local-size: {}", REROUTE_BUFFER_LOCAL_SIZE);

            this.REROUTE_BUFFER_REMOTE_SIZE = Long.parseLong(properties.getProperty("reroute.buffer-remote-size", "1048576"));
            LOG.info("Successfully loaded reroute-buffer-remote-size: {}", REROUTE_BUFFER_REMOTE_SIZE);

            this.NETTY_CHUNK_NUM = Integer.parseInt(properties.getProperty("netty.chunk-num", "5"));
            LOG.info("Successfully loaded netty-chunk-num: {}", NETTY_CHUNK_NUM);

            this.NETTY_CHUNK_ORDER = Integer.parseInt(properties.getProperty("netty.chunk-order", "11")); // 11 - 16MB, 9 - 4MB
            LOG.info("Successfully loaded netty-chunk-order: {}", NETTY_CHUNK_ORDER);

            this.CACHE_CAPACITY = Integer.parseInt(properties.getProperty("drrs.cache.capacity", "200"));
            LOG.info("Successfully loaded cache-capacity: {}", CACHE_CAPACITY);

            this.ENABLE_DR = Boolean.parseBoolean(properties.getProperty("drrs.enable-dr", "true"));
            LOG.info("Successfully loaded enable-dr: {}", ENABLE_DR);

            this.ENABLE_SUBSCALE = Boolean.parseBoolean(properties.getProperty("drrs.enable-subscale", "true"));
            LOG.info("Successfully loaded enable-subscale: {}", ENABLE_SUBSCALE);

            this.STATE_SAMPLE_RATE = Double.parseDouble(properties.getProperty("drrs.state-sample-rate", "10"));
            LOG.info("Successfully loaded state-sample-rate: {}", STATE_SAMPLE_RATE);

            this.SUBSCALE_INTERNAL_KEY_SCHEDULER = InternalKeyScheduler.valueOf(properties.getProperty("drrs.subscale.internal-key-scheduler", "Lexicographic"));
            this.MIGRATION_BUFFER_EMERGENCY_RATIO = Double.parseDouble(properties.getProperty("drrs.subscale.buffer-emergency-ratio", "0.9"));
            this.FAIRNESS_WEIGHT = Double.parseDouble(properties.getProperty("drrs.subscale.internal-key-scheduler.adaptive.fairness-weight", "0.5"));
            this.ENABLE_SUBSCALE_SCHEDULING = Boolean.parseBoolean(properties.getProperty("drrs.enable-subscale-scheduling", "false"));
            LOG.info("Successfully loaded subscale-reassignment: scheduler: {}, emergency-ratio: {}, fairness-weight: {}, enable-scheduling: {}",
                    SUBSCALE_INTERNAL_KEY_SCHEDULER, MIGRATION_BUFFER_EMERGENCY_RATIO, FAIRNESS_WEIGHT, ENABLE_SUBSCALE_SCHEDULING);



        } else {
            LOG.error("Configuration file {} does not exist or is not readable", propertiesConfig);
            throw new RuntimeException("Configuration file " + propertiesConfig + " does not exist or is not readable");
        }
    }
}
