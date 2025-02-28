package org.apache.flink.runtime.scale.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.flink.runtime.messages.webmonitor.InfoMessage;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.io.TargetOperatorMetrics;
import org.apache.flink.runtime.scale.io.network.UpstreamOperatorMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ScaleMetricsInfo implements ResponseBody, InfoMessage {
    private static final long serialVersionUID = 1L;

    public static final String PROCESSING_KC = "processing-kc";
    public static final String UPSTREAM_KC = "upstream-kc";
    public static final String Upstream_KR = "upstream-kr";
    public static final String CURRENT_STATE_LOCATION = "stateLocation";
    public static final String TASK_AVAILABLE = "taskAvailableStatus";

    // for each key group
    // 0-not involved in any subscale
    // 1-involved in ongoing subscale: not transferred yet
    // 2-involved in ongoing subscale: already transferred but not (implicit) confirmed yet
    // 3-involved in finished subscale or finished in any ongoing subscale
    public static final String MIGRATION_STATUS = "migrationStatus"; 

    @JsonProperty(PROCESSING_KC)
    private final Long[] processingKeyCounts;
    @JsonProperty(UPSTREAM_KC)
    private final Long[] upstreamKeyCounts;
    @JsonProperty(Upstream_KR)
    private final Double[] upstreamKeyRates;

    @JsonProperty(CURRENT_STATE_LOCATION)
    private final List<Integer> stateLocation;

    @JsonProperty(TASK_AVAILABLE)
    private final List<Boolean> taskAvailableStatus;

    @JsonProperty(MIGRATION_STATUS)
    private final List<Integer> migrationStatus;



    @JsonCreator
    public ScaleMetricsInfo(
            @JsonProperty(UPSTREAM_KC) Long[] upstreamKeyCounts,
            @JsonProperty(Upstream_KR) Double[] upstreamKeyRates,
            @JsonProperty(PROCESSING_KC) Long[] processingKeyCounts,
            @JsonProperty(CURRENT_STATE_LOCATION) List<Integer> currentStateLocation,
            @JsonProperty(TASK_AVAILABLE) List<Boolean> taskAvailableStatus,
            @JsonProperty(MIGRATION_STATUS) List<Integer> migrationStatus){

        this.upstreamKeyCounts = upstreamKeyCounts;
        this.upstreamKeyRates = upstreamKeyRates;
        this.processingKeyCounts = processingKeyCounts;
        this.stateLocation = currentStateLocation;
        this.taskAvailableStatus = taskAvailableStatus;
        this.migrationStatus = migrationStatus;
    }

    public static ScaleMetricsInfo of(
            List<CompletableFuture<UpstreamOperatorMetrics>> upstreamMetricFutures,
            List<CompletableFuture<TargetOperatorMetrics>> scaleMetricFutures,
            int maxKeyGroupIndex,
            int newParallelism){

        // -------------------- processing upstream metrics --------------------

        Double[] upstreamKeyRatesList = new Double[maxKeyGroupIndex];
        Arrays.fill(upstreamKeyRatesList, 0.0);
        Long[] upstreamKeyCountsList = new Long[maxKeyGroupIndex];
        Arrays.fill(upstreamKeyCountsList, 0L);
        if (ScaleConfig.Instance.ENABLE_SUBSCALE_SCHEDULING){
            for (CompletableFuture<UpstreamOperatorMetrics> future : upstreamMetricFutures){
                UpstreamOperatorMetrics m = future.join();
                for (int i=0; i<maxKeyGroupIndex; i++){
                    upstreamKeyCountsList[i] += m.upstreamKeyCounts[i];
                    upstreamKeyRatesList[i] += m.upstreamRates[i];
                }
            }
        }


        // -------------------- processing target metrics --------------------
        List<TargetOperatorMetrics> targetOperatorMetrics = scaleMetricFutures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        Long[] processingKeyCountsList = new Long[maxKeyGroupIndex];
        Arrays.fill(processingKeyCountsList, 0L);

        // 1. calculate hotness by channel
        if(ScaleConfig.Instance.ENABLE_SUBSCALE_SCHEDULING){
            for (TargetOperatorMetrics m : targetOperatorMetrics) {
                Long[] keyCounts = m.keyCounts;
                for (int i=0; i<maxKeyGroupIndex; i++){
                    processingKeyCountsList[i] += keyCounts[i];
                }
            }
        }

        // 2. calculate  currentStateLocation
        List<Integer> currentStateLocation = new ArrayList<>(maxKeyGroupIndex);
        for (int i = 0; i < maxKeyGroupIndex; i++){
            currentStateLocation.add(-1);
        }
        for (int taskIndex = 0; taskIndex < targetOperatorMetrics.size(); taskIndex++){
            TargetOperatorMetrics m = targetOperatorMetrics.get(taskIndex);
            for (int keyGroup : m.holdingKeys){
                currentStateLocation.set(keyGroup, taskIndex);
            }
        }

        // 3. calculate taskAvailableStatus
        List<Boolean> taskAvailableStatus = new ArrayList<>(maxKeyGroupIndex);
        for (TargetOperatorMetrics m : targetOperatorMetrics){
            taskAvailableStatus.add(m.isAvailable);
        }
        while (taskAvailableStatus.size() < newParallelism){
            taskAvailableStatus.add(false); // for new tasks that are not started yet
        }

        // 4. calculate migrationStatus
        List<Integer> migrationStatus = new ArrayList<>(maxKeyGroupIndex);
        for (int i = 0; i < maxKeyGroupIndex; i++){
            migrationStatus.add(0);
        }
        for (TargetOperatorMetrics m : targetOperatorMetrics) {
            m.migrationStatus.forEach(migrationStatus::set);
        }

        return new ScaleMetricsInfo(
                upstreamKeyCountsList,
                upstreamKeyRatesList,
                processingKeyCountsList,
                currentStateLocation,
                taskAvailableStatus,
                migrationStatus);
    }

    public Long[] getProcessingKeyCounts() {
        return processingKeyCounts;
    }
    public Double[] getUpstreamKeyRates() {
        return upstreamKeyRates;
    }
    public Long[] getUpstreamKeyCounts() {
        return upstreamKeyCounts;
    }
    public List<Integer> getStateLocation() {
        return stateLocation;
    }
    public List<Boolean> getTaskAvailableStatus() { return taskAvailableStatus; }
    public List<Integer> getMigrationStatus() {
        return migrationStatus;
    }



}
