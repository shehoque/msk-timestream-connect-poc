package com.aws.kafka.connect.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aws.kafka.connect.common.ConnectMetadataUtil.getVersion;
import static com.aws.kafka.connect.sink.TimeStreamSinkConfig.*;

@Slf4j
public class TimeStreamSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public void start(final Map<String, String> properties) {
        log.info("Starting TimestreamConnector with properties {}", properties);
        configProps = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TimeStreamSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            final Map<String, String> taskConfig = new HashMap<>(configProps);
            // add task specific values
            taskConfig.put(TASK_ID, String.valueOf(i));
            taskConfig.put(TASK_MAX, String.valueOf(maxTasks));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping TimestreamConnector.");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return getVersion();
    }
}
