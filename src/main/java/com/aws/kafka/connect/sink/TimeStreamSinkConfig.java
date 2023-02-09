package com.aws.kafka.connect.sink;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

public class TimeStreamSinkConfig extends AbstractConfig {

    public static final String MAX_CONNECTIONS = "connections.max_connection";
    public static final String NUM_RETRIES = "connections.retries";
    public static final String MAX_TIMEOUT_SECONDS = "connections.timeout_seconds";
    public static final String DATABASE_NAME = "timestream.database.name";
    public static final String TABLE_NAME = "timestream.table.name";
    public static final String AWS_REGION = "aws.region";
    public static final String TASK_ID = "task.id";
    public static final String TASK_MAX = "task.max";

    public static final int MAX_CONNECTIONS_DEFAULT = 100;
    public static final int NUM_RETRIES_DEFAULT = 10;
    public static final int MAX_TIMEOUT_SECONDS_DEFAULT = 20;

    public static final int TASK_MAX_DEFAULT = 1;

    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(DATABASE_NAME,
                            ConfigDef.Type.STRING,
                            "anaplan-poc-db",
                            ConfigDef.Importance.HIGH,
                            "database to connect")
                    .define(TABLE_NAME,
                            ConfigDef.Type.STRING,
                            "clickstream-poc-tbl",
                            ConfigDef.Importance.HIGH,
                            "table name")
                    .define(AWS_REGION,
                            ConfigDef.Type.STRING,
                            "us-east-2",
                            ConfigDef.Importance.HIGH,
                            "AWS region of timestream database")
                    .define(MAX_CONNECTIONS,
                            ConfigDef.Type.INT,
                            MAX_CONNECTIONS_DEFAULT,
                            ConfigDef.Importance.HIGH,
                            "Maximum number of connections")
                    .define(NUM_RETRIES,
                            ConfigDef.Type.INT,
                            NUM_RETRIES_DEFAULT,
                            ConfigDef.Importance.HIGH,
                            "Max number of retry.")
                    .define(MAX_TIMEOUT_SECONDS,
                            ConfigDef.Type.INT,
                            MAX_TIMEOUT_SECONDS_DEFAULT,
                            ConfigDef.Importance.HIGH,
                            "Timeout in seconds")
                    .define(TASK_MAX,
                            ConfigDef.Type.INT,
                            TASK_MAX_DEFAULT,
                            ConfigDef.Importance.HIGH,
                            "maximum number of tasks");


    public TimeStreamSinkConfig(final Map<?, ?> properties) {
        super(CONFIG_DEF, properties);
    }

    public int getMaxConnections() {
        try {
            return getInt(MAX_CONNECTIONS);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public Region getAWSRegion() {
        try {
            return Region.of(getString(AWS_REGION));
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public int getNumRetries() {
        try {
            return getInt(NUM_RETRIES);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public int getMaxTimeoutSeconds() {
        try {
            return getInt(MAX_TIMEOUT_SECONDS);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public int getTaskMax() {
        try {
            return getInt(TASK_MAX);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public int getTaskId() {
        try {
            return getInt(TASK_ID);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public String getDatabaseName(){
        try {
            return getString(DATABASE_NAME);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    public String getTableName(){
        try {
            return getString(TABLE_NAME);
        } catch (final IllegalArgumentException ex) {
            throw new ConfigException("Configuration error.", ex);
        }
    }

    /**
     * for now only glue schema registry, later we can extend for confluent registry as well
     * @return
     */
    public Map<String, Object> getGSRConfigs() {
        Map<String, Object> gsrConfigs = new HashMap<>();
        gsrConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        gsrConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AWSKafkaAvroDeserializer.class.getName());
        gsrConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, getAWSRegion().toString());
        gsrConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
        return gsrConfigs;
    }


}
