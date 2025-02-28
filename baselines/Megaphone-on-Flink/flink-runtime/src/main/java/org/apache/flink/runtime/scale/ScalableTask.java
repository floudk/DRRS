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

package org.apache.flink.runtime.scale;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ScalableTask {

    void expandRecordWriters(ResultPartitionID resultPartitionID);

    void expandInputGates(
            SingleInputGate inputGate, List<InputChannel> addedInputChannels) throws Exception;

    void setInvokableRestoreFuture(CompletableFuture<Void> invokableRestoreFuture);


    void resetScale() throws IOException;

    void triggerSubscale(Map<Integer,Integer> involvedKeyGroups);

    // sync signals
    void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException;

    void onCompleteBarrier();
}
