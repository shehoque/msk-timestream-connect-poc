package com.aws.kafka.connect.sink.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class TimeStreamSimpleWriter {
    public static final long HT_TTL_HOURS = 24L;
    public static final long CT_TTL_DAYS = 7L;

    private final TimestreamWriteClient timestreamWriteClient;

    private final Random random = new Random();
    private final String databaseName;
    private final String tableName;
    private final Deserializer deserializer;

    private final String taskId;

    public TimeStreamSimpleWriter(TimestreamWriteClient client, String databaseName, String tableName, String taskId, Deserializer deserializer) {
        this.timestreamWriteClient = client;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.deserializer = deserializer;
        this.taskId = taskId;
    }

    private byte[] base64Decode( String value) {
        return Base64.getDecoder().decode((value.getBytes(StandardCharsets.UTF_8)));
    }

    public void writeRecord(SinkRecord sinkRecord) {
        Struct struct = (Struct)sinkRecord.value();
        List<Record> records = new ArrayList<>();
        List<Dimension> dimensions = new ArrayList<>();
        Dimension workspaceId = Dimension.builder().name("WorkspaceId").value(struct.getString("workspaceId")).build();
        Dimension modelId= Dimension.builder().name("ModelId").value(struct.getString("modelId")).build();
        Dimension state = Dimension.builder().name("State").value(struct.getString("state")).build();
        Dimension version = Dimension.builder().name("Version").value(struct.getString("version")).build();
        dimensions.add(workspaceId);
        dimensions.add(modelId);
        dimensions.add(state);
        dimensions.add(version);
        Collection<MeasureValue> measureValues = new ArrayList<>();
        MeasureValue memoryUtilMV = MeasureValue.builder().name("Mem_Util").value(String.valueOf(struct.getFloat64("memUtil"))).type(MeasureValueType.DOUBLE).build();
        MeasureValue cpuUtilMV = MeasureValue.builder().name("Cpu_Util").value(String.valueOf(struct.getFloat64("cpuUtl"))).type(MeasureValueType.DOUBLE).build();
        measureValues.add(memoryUtilMV);
        measureValues.add(cpuUtilMV);
        Record workspaceMV = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.MULTI)
                .measureName("Metrics")
                .measureValues(measureValues)
                .time(String.valueOf(struct.getInt64("eventTimestamp"))).timeUnit(TimeUnit.SECONDS).build();

        records.add(workspaceMV);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(databaseName).tableName(tableName).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            log.info("WriteRecords Status: {}" , writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            log.error("Error: " , e);
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        log.error("RejectedRecords: " , e);
        e.rejectedRecords().forEach(System.out::println);
    }
}
