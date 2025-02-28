package org.apache.flink.runtime.scale.io;

import org.apache.flink.runtime.scale.ScalingContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TargetOperatorMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    public final Long[] keyCounts; // channel->keyGroup->recentRecordCount

    public List<Integer> holdingKeys; // keyGroup in current task

    public final boolean isAvailable;

    // 0-not involved in any subscale(by default)
    // 1-involved in ongoing subscale
    // 2-involved in finished subscale or ongoing subscale(but already finished)
    public final Map<Integer,Integer> migrationStatus; // keyGroup->status

    public TargetOperatorMetrics(Long[] keyCounts,
                                 ScalingContext scalingContext,
                                 boolean isAvailable){
        this.keyCounts = keyCounts;
        try {
            this.holdingKeys = scalingContext.getLocalKeyGroups();
        } catch (Exception e) {
            // may be null since the async operation
            // just use the default value keyCounts>0
            this.holdingKeys = new ArrayList<>();
            if(keyCounts!=null){
                for (int i = 0; i < keyCounts.length; i++) {
                    if (keyCounts[i] > 0) {
                        this.holdingKeys.add(i);
                    }
                }
            }
        }

        this.isAvailable = isAvailable;
        this.migrationStatus = scalingContext.getMigrationStatus();
    }
    @Override
    public String toString() {
        return "ScaleMetric{" + migrationStatus + '}';
    }
}
