package com.aws.kafka.connect.sink;

import lombok.Builder;
import lombok.Data;
import software.amazon.awssdk.regions.Region;

@Data
@Builder
public class TimeStreamSettings {
    private int maxConnections;
    private int numRetries;
    private int timeoutInSeconds;
    private Region region;
}
