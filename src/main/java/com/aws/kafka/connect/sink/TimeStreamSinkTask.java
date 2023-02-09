package com.aws.kafka.connect.sink;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.aws.kafka.connect.sink.writer.TimeStreamSimpleWriter;
import com.aws.kafka.connect.sink.writer.TimeStreamWriterClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

import java.util.Collection;
import java.util.Map;

import static com.aws.kafka.connect.common.ConnectMetadataUtil.getVersion;
import static com.aws.kafka.connect.sink.TimeStreamSinkConfig.TASK_ID;

@Slf4j
public class TimeStreamSinkTask extends SinkTask {

    private TimeStreamSimpleWriter timeStreamSimpleWriter;

    @Override
    public String version() {
        return getVersion();
    }

    @Override
    public void start(final Map<String, String> properties) {
        final TimeStreamSinkConfig config = new TimeStreamSinkConfig(properties);
        final TimeStreamSettings timeStreamSettings = buildSettings(config);
        final TimestreamWriteClient timestreamWriteClient = TimeStreamWriterClientFactory.getTimeStreamWriterClient(timeStreamSettings);
        final Deserializer deserializer = new AWSKafkaAvroDeserializer(config.getGSRConfigs());
        log.info("Starting TimeStreamSinkTask with properties {}", properties);
        timeStreamSimpleWriter = new TimeStreamSimpleWriter(timestreamWriteClient, config.getDatabaseName(), config.getTableName(), properties.get(TASK_ID), deserializer);
    }

    private TimeStreamSettings buildSettings(TimeStreamSinkConfig config) {
        return TimeStreamSettings.builder()
                .maxConnections(config.getMaxConnections())
                .numRetries(config.getNumRetries())
                .region(config.getAWSRegion())
                .timeoutInSeconds(config.getMaxTimeoutSeconds())
                .build();
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
       for(SinkRecord sinkRecord:records){
           log.info("record - {}", sinkRecord);
           timeStreamSimpleWriter.writeRecord(sinkRecord);
       }
    }




    @Override
    public void stop() {
        log.info("Stopping TimestreamSink.");
    }
}
