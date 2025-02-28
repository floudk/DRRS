package org.apache.flink.runtime.scale.io.network;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class UpstreamOperatorMetrics implements Serializable {
    public final Long[] upstreamKeyCounts;
    public final Double[] upstreamRates;

    public UpstreamOperatorMetrics(Tuple2<Long[], Double[]> upstream) {
        this.upstreamKeyCounts = upstream.f0;
        this.upstreamRates = upstream.f1;
    }
}
