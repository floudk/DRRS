/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashSet;

/** Tracks status of upstream channels while they recover. */
@Internal
@VisibleForTesting
public interface UpstreamRecoveryTracker {

    void handleEndOfRecovery(InputChannelInfo channelInfo) throws IOException;

    boolean allChannelsRecovered();

    static UpstreamRecoveryTracker forInputGate(InputGate inputGate) {
        return new UpstreamRecoveryTrackerImpl(inputGate);
    }

    UpstreamRecoveryTracker NO_OP =
            new UpstreamRecoveryTracker() {
                @Override
                public void handleEndOfRecovery(InputChannelInfo channelInfo) {}

                @Override
                public boolean allChannelsRecovered() {
                    return true;
                }
            };
}

final class UpstreamRecoveryTrackerImpl implements UpstreamRecoveryTracker {
    private final HashSet<InputChannelInfo> restoredChannels;
    private int numUnrestoredChannels;
    private final InputGate inputGate;
    private boolean isupdating = false;

    UpstreamRecoveryTrackerImpl(InputGate inputGate) {
        this.restoredChannels = new HashSet<>();
        this.numUnrestoredChannels = inputGate.getNumberOfInputChannels();
        this.inputGate = inputGate;
    }

    @Override
    public void handleEndOfRecovery(InputChannelInfo channelInfo) throws IOException {
        if(inputGate instanceof InputGateWithMetrics){
            if(inputGate.isUpdatingForUpstreamScaling()){
                if(!isupdating){
                    numUnrestoredChannels = ((InputGateWithMetrics) inputGate).getNewlyAddedChannelNumber();
                    isupdating = true;
                }
            }
        }

        if (numUnrestoredChannels > 0) {
            Preconditions.checkState(
                    !restoredChannels.contains(channelInfo), "already restored: %s", channelInfo);
            restoredChannels.add(channelInfo);
            numUnrestoredChannels--;
            if (numUnrestoredChannels == 0) {
                if(isupdating){
                    for(InputChannelInfo inputChannelInfo : restoredChannels){
                        inputGate.resumeConsumption(inputChannelInfo);
                    }
                    ((InputGateWithMetrics) inputGate).unsetUpdatingForUpstreamScaling();
                    isupdating = false;
                }else{
                    for (InputChannelInfo inputChannelInfo : inputGate.getChannelInfos()) {
                        inputGate.resumeConsumption(inputChannelInfo);
                    }
                }
                restoredChannels.clear();
            }
        }
    }

    @Override
    public boolean allChannelsRecovered() {
        return numUnrestoredChannels == 0;
    }
}
