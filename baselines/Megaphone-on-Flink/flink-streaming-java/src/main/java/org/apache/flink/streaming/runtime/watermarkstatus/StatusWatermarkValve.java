/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue.HeapPriorityQueueElement;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link
 * WatermarkStatus} are propagated to downstream outputs, given a set of one or multiple input
 * channels that continuously receive them. Usages of this class need to define the number of input
 * channels that the valve needs to handle, as well as provide a implementation of {@link
 * DataOutput}, which is called by the valve only when it determines a new watermark or watermark
 * status can be propagated.
 */
@Internal
public class StatusWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * Array of current status of all input channels. Changes as watermarks & watermark statuses are
     * fed into the valve.
     */
    private InputChannelStatus[] channelStatuses;

    /** The last watermark emitted from the valve. */
    private long lastOutputWatermark;

    /** The last watermark status emitted from the valve. */
    private WatermarkStatus lastOutputWatermarkStatus;

    /** A heap-based priority queue to help find the minimum watermark. */
    private final HeapPriorityQueue<InputChannelStatus> alignedChannelStatuses;

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    public StatusWatermarkValve(int numInputChannels) {
        checkArgument(numInputChannels > 0);
        this.channelStatuses = new InputChannelStatus[numInputChannels];
        this.alignedChannelStatuses =
                new HeapPriorityQueue<>(
                        (left, right) -> Long.compare(left.watermark, right.watermark),
                        numInputChannels);
        for (int i = 0; i < numInputChannels; i++) {
            channelStatuses[i] = new InputChannelStatus();
            channelStatuses[i].watermark = Long.MIN_VALUE;
            channelStatuses[i].watermarkStatus = WatermarkStatus.ACTIVE;
            markWatermarkAligned(channelStatuses[i]);
        }

        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     */
    public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output)
            throws Exception {
        // ignore the input watermark if its input channel, or all input channels are idle (i.e.
        // overall the valve is idle).
        if (lastOutputWatermarkStatus.isActive()
                && channelStatuses[channelIndex].watermarkStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its input
            // channel, ignore it also.
            if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                channelStatuses[channelIndex].watermark = watermarkMillis;

                if (channelStatuses[channelIndex].isWatermarkAligned) {
                    adjustAlignedChannelStatuses(channelStatuses[channelIndex]);
                } else if (watermarkMillis >= lastOutputWatermark) {
                    // previously unaligned input channels are now aligned if its watermark has
                    // caught up
                    markWatermarkAligned(channelStatuses[channelIndex]);
                }

                // now, attempt to find a new min watermark across all aligned channels
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        }
    }

    /**
     * Feed a {@link WatermarkStatus} into the valve. This may trigger the valve to output either a
     * new Watermark Status, for which {@link DataOutput#emitWatermarkStatus(WatermarkStatus)} will
     * be called, or a new Watermark, for which {@link DataOutput#emitWatermark(Watermark)} will be
     * called.
     *
     * @param watermarkStatus the watermark status to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark status belongs to (index
     *     starting from 0)
     */
    public void inputWatermarkStatus(
            WatermarkStatus watermarkStatus, int channelIndex, DataOutput<?> output)
            throws Exception {
        // only account for watermark status inputs that will result in a status change for the
        // input
        // channel
        if (watermarkStatus.isIdle() && channelStatuses[channelIndex].watermarkStatus.isActive()) {
            // handle active -> idle toggle for the input channel
            channelStatuses[channelIndex].watermarkStatus = WatermarkStatus.IDLE;

            // the channel is now idle, therefore not aligned
            markWatermarkUnaligned(channelStatuses[channelIndex]);

            // if all input channels of the valve are now idle, we need to output an idle stream
            // status from the valve (this also marks the valve as idle)
            if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

                // now that all input channels are idle and no channels will continue to advance its
                // watermark,
                // we should "flush" all watermarks across all channels; effectively, this means
                // emitting
                // the max watermark across all channels as the new watermark. Also, since we
                // already try to advance
                // the min watermark as channels individually become IDLE, here we only need to
                // perform the flush
                // if the watermark of the last active channel that just became idle is the current
                // min watermark.
                if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
                    findAndOutputMaxWatermarkAcrossAllChannels(output);
                }

                lastOutputWatermarkStatus = WatermarkStatus.IDLE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            } else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
                // if the watermark of the channel that just became idle equals the last output
                // watermark (the previous overall min watermark), we may be able to find a new
                // min watermark from the remaining aligned channels
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        } else if (watermarkStatus.isActive()
                && channelStatuses[channelIndex].watermarkStatus.isIdle()) {
            // handle idle -> active toggle for the input channel
            channelStatuses[channelIndex].watermarkStatus = WatermarkStatus.ACTIVE;

            // if the last watermark of the input channel, before it was marked idle, is still
            // larger than
            // the overall last output watermark of the valve, then we can set the channel to be
            // aligned already.
            if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
                markWatermarkAligned(channelStatuses[channelIndex]);
            }

            // if the valve was previously marked to be idle, mark it as active and output an active
            // stream
            // status because at least one of the input channels is now active
            if (lastOutputWatermarkStatus.isIdle()) {
                lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            }
        }
    }

    private void findAndOutputNewMinWatermarkAcrossAlignedChannels(DataOutput<?> output)
            throws Exception {
        boolean hasAlignedChannels = !alignedChannelStatuses.isEmpty();

        // we acknowledge and output the new overall watermark if it really is aggregated
        // from some remaining aligned channel, and is also larger than the last output watermark
        if (hasAlignedChannels && alignedChannelStatuses.peek().watermark > lastOutputWatermark) {
            lastOutputWatermark = alignedChannelStatuses.peek().watermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * Mark the {@link InputChannelStatus} as watermark-aligned and add it to the {@link
     * #alignedChannelStatuses}.
     *
     * @param inputChannelStatus the input channel status to be marked
     */
    private void markWatermarkAligned(InputChannelStatus inputChannelStatus) {
        inputChannelStatus.isWatermarkAligned = true;
        inputChannelStatus.addTo(alignedChannelStatuses);
    }

    /**
     * Mark the {@link InputChannelStatus} as watermark-unaligned and remove it from the {@link
     * #alignedChannelStatuses}.
     *
     * @param inputChannelStatus the input channel status to be marked
     */
    private void markWatermarkUnaligned(InputChannelStatus inputChannelStatus) {
        inputChannelStatus.isWatermarkAligned = false;
        inputChannelStatus.removeFrom(alignedChannelStatuses);
    }

    /**
     * Adjust the {@link #alignedChannelStatuses} when an element({@link InputChannelStatus}) in it
     * was modified. The {@link #alignedChannelStatuses} is a priority queue, when an element in it
     * was modified, we need to adjust the element's position to ensure its priority order.
     *
     * @param inputChannelStatus the modified input channel status
     */
    private void adjustAlignedChannelStatuses(InputChannelStatus inputChannelStatus) {
        alignedChannelStatuses.adjustModifiedElement(inputChannelStatus);
    }

    private void findAndOutputMaxWatermarkAcrossAllChannels(DataOutput<?> output) throws Exception {
        long maxWatermark = Long.MIN_VALUE;

        for (InputChannelStatus channelStatus : channelStatuses) {
            maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
        }

        if (maxWatermark > lastOutputWatermark) {
            lastOutputWatermark = maxWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
     * status, and whether or not the channel's current watermark is aligned with the overall
     * watermark output from the valve.
     *
     * <p>There are 2 situations where a channel's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the channel is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the channel hasn't
     *       caught up to the last output watermark from the valve yet.
     * </ul>
     *
     * <p>NOTE: This class implements {@link HeapPriorityQueueElement} to be managed by {@link
     * #alignedChannelStatuses} to help find minimum watermark.
     */
    @VisibleForTesting
    protected static class InputChannelStatus implements HeapPriorityQueueElement {
        protected long watermark;
        protected WatermarkStatus watermarkStatus;
        protected boolean isWatermarkAligned;

        /**
         * This field holds the current physical index of this channel status when it is managed by
         * a {@link HeapPriorityQueue}.
         */
        private int heapIndex = HeapPriorityQueueElement.NOT_CONTAINED;

        /**
         * Utility to check if at least one channel in a given array of input channels is active.
         */
        private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.watermarkStatus.isActive()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int getInternalIndex() {
            return heapIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.heapIndex = newIndex;
        }

        private void removeFrom(HeapPriorityQueue<InputChannelStatus> queue) {
            queue.remove(this);
            setInternalIndex(HeapPriorityQueueElement.NOT_CONTAINED);
        }

        private void addTo(HeapPriorityQueue<InputChannelStatus> queue) {
            // Check the heap index to avoid the same object being added repeatedly
            if (heapIndex == HeapPriorityQueueElement.NOT_CONTAINED) {
                queue.add(this);
            }
        }
    }

    @VisibleForTesting
    protected InputChannelStatus getInputChannelStatus(int channelIndex) {
        Preconditions.checkArgument(
                channelIndex >= 0 && channelIndex < channelStatuses.length,
                "Invalid channel index. Number of input channels: " + channelStatuses.length);

        return channelStatuses[channelIndex];
    }

    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------
    public void expandChannelStatuses(int newChannelNum){

        InputChannelStatus[] newChannelStatuses = new InputChannelStatus[newChannelNum];
        int oldChannelNum = channelStatuses.length;
        System.arraycopy(channelStatuses, 0, newChannelStatuses, 0, oldChannelNum);
        for(int i=oldChannelNum;i<newChannelNum;i++){
            newChannelStatuses[i] = new InputChannelStatus();
            newChannelStatuses[i].watermark = Long.MIN_VALUE;
            newChannelStatuses[i].watermarkStatus = WatermarkStatus.ACTIVE;
            markWatermarkAligned(newChannelStatuses[i]);
        }

        this.channelStatuses = newChannelStatuses;
    }
}
