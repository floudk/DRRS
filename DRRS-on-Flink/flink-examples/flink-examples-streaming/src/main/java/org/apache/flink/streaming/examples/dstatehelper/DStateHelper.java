package org.apache.flink.streaming.examples.dstatehelper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;

public class DStateHelper {
    private final static Logger LOG = LoggerFactory.getLogger(DStateHelper.class);

    enum ProcessingTimeDistribution {
        UNIFORM, NORMAL, ZIPF
    }
    enum RecordsPerKeyDistribution {
        EVENLY, STATE_SIZE, ZIPF
    }
    enum StateSizeDistribution {
        UNIFORM, NORMAL, ZIPF
    }
    enum InputDistribution {
        EVENLY, UNEVENLY
    }

    // global random generator so that the program is deterministic
    final static Random random = new Random(42);

    static int sourceParallelism = 1;
    static int sinkParallelism = 1;

    static int totalRuntime; // seconds
    static List<String> keyList;
    // ------------------- records arguments -------------------
    static int inputRate; // items/s
    static int recordSize; // bytes
    static InputDistribution inputDistribution; // input distribution in multiple upstreams

    static ProcessingTimeDistribution processingTimeDistribution = ProcessingTimeDistribution.UNIFORM;
    static double processingTimeMean = 50; // ms
    static double processingTimeStddev = 30; // ms

    static RecordsPerKeyDistribution recordsPerKeyDistribution = RecordsPerKeyDistribution.EVENLY;
    static double keySkewFactor = 0.0; // 0.0 means no skew

    // ------------------- state arguments -------------------
    static StateSizeDistribution stateSizeDistribution = StateSizeDistribution.UNIFORM;
    static int stateSizeMean = 5 * 1024 * 1024; // bytes (5MB)

    static int maxKeyNum = 12;

    static boolean keepAliveAfterEmitComplete = false;

    public static void main(String[] args) throws Exception{
        CLI.fromArgs(args);

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining(); // ban the chain optimization

        // Source Operator (parallelism = 1),
        // input message 'msg' to following operator 1 item/s and running for 6s
        DataStream<Tuple5<String, Integer, Long, Long, byte[]>> sourceStream = env.addSource(
                new MySource(
                        totalRuntime,
                        keyList,
                        inputRate,
                        inputDistribution,
                        sourceParallelism,
                        recordSize,
                        processingTimeDistribution,
                        processingTimeMean,
                        processingTimeStddev,
                        recordsPerKeyDistribution,
                        keySkewFactor,
                        keepAliveAfterEmitComplete)
                ).setParallelism(sourceParallelism).name("source").slotSharingGroup("exclusive-source");


        KeyedStream<Tuple5<String, Integer, Long, Long, byte[]>, String> keyedStream =
                sourceStream.keyBy(
                        (KeySelector<Tuple5<String, Integer, Long, Long, byte[]>, String>) value -> value.f0.split("_")[0]);

        DataStream <Tuple3<String, Integer, Tuple3<Long, Long, Long>>> resultStream = keyedStream
                .map(new MyStateOp(keyList, stateSizeDistribution, stateSizeMean))
                .name("count").setMaxParallelism(maxKeyNum).slotSharingGroup("exclusive-count");

        FileSink<Tuple3<String, Integer , Tuple3<Long, Long, Long>>> fileSink = FileSink
                .forRowFormat(new Path("file:///opt/output"), new PrintStyleEncoder())
                .build();


        // print the result.f0 and result.f1 to stdout
        resultStream.keyBy(value -> value.f0).
                sinkTo(fileSink).
                name("FileSink").
                setParallelism(sinkParallelism).
                slotSharingGroup("exclusive-print");

        // execute the program
        env.execute("State Try");

    }

    private static class PrintStyleEncoder implements Encoder<Tuple3<String, Integer, Tuple3<Long, Long, Long>>> {
        @Override
        public void encode(Tuple3<String, Integer, Tuple3<Long, Long, Long>> value, OutputStream stream) throws IOException {
            String formattedString = "KeyWithInfo: " + value.f0 + ", State length: " + value.f1 + ", expectedGenTime: " + value.f2.f0 + ", realGenTime: " + value.f2.f1 + ", processedTime: " + value.f2.f2 + "\n";
            stream.write(formattedString.getBytes(StandardCharsets.UTF_8));
        }
    }

    static class CLI {
        static final String RUNTIME = "runtime";
        static final String MAX_KEY_NUM = "maxKeyNum";
        static final String KEY_NUM = "keyNum";
        static final String INPUT_RATE = "inputRate";
        static final String INPUT_DISTRIBUTION = "inputDistribution";
        static final String RECORD_SIZE = "recordSize";
        static final String PROCESSING_TIME_DISTRIBUTION = "processingTimeDistribution";
        static final String PROCESSING_TIME_MEAN = "processingTimeMean";
        static final String PROCESSING_TIME_STDDEV = "processingTimeStddev";
        static final String RECORDS_PER_KEY_DISTRIBUTION = "recordsPerKeyDistribution";
        static final String STATE_SIZE_DISTRIBUTION = "stateSizeDistribution";
        static final String KEY_SKEW_FACTOR = "keySkewFactor";
        static final String STATE_SIZE_MEAN = "stateSizeMean";
        static final String SOURCE_PARALLELISM = "sourceParallelism";
        static final String SINK_PARALLELISM = "sinkParallelism";
        static final String KEEP_ALIVE_AFTER_EMIT_COMPLETE = "keepAliveAfterEmitComplete";



        static void fromArgs(String[] args)  {
            MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
            if(params.has(RUNTIME)){
                totalRuntime = params.getInt(RUNTIME);
            }
            if(params.has(MAX_KEY_NUM)){
                maxKeyNum = params.getInt(MAX_KEY_NUM);
            }
            if(params.has(KEY_NUM)){
                int keyNum = params.getInt(KEY_NUM);
                keyList = new ArrayList<>();
                for (int i = 0; i < keyNum; i++) {
                    // random generate keyNum keys
                    keyList.add("k" + i);
                }
            }
            if(params.has(INPUT_RATE)){
                inputRate = params.getInt(INPUT_RATE);
            }
            if(params.has(INPUT_DISTRIBUTION)){
                inputDistribution = InputDistribution.valueOf(params.get(INPUT_DISTRIBUTION));
            }
            if(params.has(RECORD_SIZE)){
                recordSize = params.getInt(RECORD_SIZE);
            }
            if(params.has(PROCESSING_TIME_DISTRIBUTION)){
                processingTimeDistribution = ProcessingTimeDistribution.valueOf(params.get(PROCESSING_TIME_DISTRIBUTION));
            }
            if(params.has(PROCESSING_TIME_MEAN)){
                processingTimeMean = params.getDouble(PROCESSING_TIME_MEAN);
            }
            if(params.has(PROCESSING_TIME_STDDEV)){
                processingTimeStddev = params.getDouble(PROCESSING_TIME_STDDEV);
            }
            if(params.has(RECORDS_PER_KEY_DISTRIBUTION)){
                recordsPerKeyDistribution = RecordsPerKeyDistribution.valueOf(params.get(RECORDS_PER_KEY_DISTRIBUTION));
            }
            if(params.has(STATE_SIZE_DISTRIBUTION)){
                stateSizeDistribution = StateSizeDistribution.valueOf(params.get(STATE_SIZE_DISTRIBUTION));
            }
            if(params.has(STATE_SIZE_MEAN)){
                stateSizeMean = params.getInt(STATE_SIZE_MEAN);
            }
            if(params.has(SOURCE_PARALLELISM)){
                sourceParallelism = params.getInt(SOURCE_PARALLELISM);
            }
            if(params.has(SINK_PARALLELISM)){
                sinkParallelism = params.getInt(SINK_PARALLELISM);
            }
            if (params.has(KEY_SKEW_FACTOR)) {
                keySkewFactor = params.getDouble(KEY_SKEW_FACTOR);
            }
            if (params.has(KEEP_ALIVE_AFTER_EMIT_COMPLETE)) {
                keepAliveAfterEmitComplete = params.getBoolean(KEEP_ALIVE_AFTER_EMIT_COMPLETE);
            }

            System.out.println("Configuration loaded successfully");
            System.out.println("totalRuntime: " + totalRuntime);
            System.out.println("keyList: " + keyList);
            System.out.println("inputRate: " + inputRate);
            System.out.println("records_per_key_distribution: " + recordsPerKeyDistribution +", keySkewFactor: " + keySkewFactor);
            System.out.println("recordSize: " + recordSize);

        }
    }


    static class MyStateOp extends
            RichMapFunction<Tuple5<String, Integer, Long, Long, byte[]>, Tuple3<String, Integer, Tuple3<Long, Long, Long>>> {

        final StateSizeDistribution stateSizeDistribution;
        final int stateSizeMean;
        final Map<String, Integer> stateSizeList = new HashMap<>();

        private transient ValueState<List<byte[]>> stateBytesList;

        MyStateOp(List<String> keyList, StateSizeDistribution stateSizeDistribution, int stateSizeMean) {
            this.stateSizeDistribution = stateSizeDistribution;
            this.stateSizeMean = stateSizeMean;

            for (String key : keyList) {
                int stateSize = generateStateSize();
                stateSizeList.put(key, stateSize);
            }
        }

        private int generateStateSize() {
            switch (stateSizeDistribution) {
                case UNIFORM:
                    return random.nextInt(stateSizeMean * 2) + 1;
                case NORMAL:
                case ZIPF:
                    throw new UnsupportedOperationException("Not implemented yet");
                default:
                    throw new IllegalArgumentException(
                            "Unknown stateSizeDistribution: " + stateSizeDistribution);
            }
        }


        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<List<byte[]>> stateDescriptor = new ValueStateDescriptor<>(
                    "count", TypeInformation.of(new TypeHint<List<byte[]>>() {}));
            stateBytesList = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple3<String, Integer, Tuple3<Long, Long, Long>> map(Tuple5<String, Integer, Long, Long, byte[]> value) throws Exception {
            long currentNanoTime = System.nanoTime();
            List<byte[]> currentState = stateBytesList.value();
            if (currentState == null) {
                int desiredStateSize = stateSizeList.get(value.f0.split("_")[0]);
                currentState = new ArrayList<>();

                int fillSize = (int) (desiredStateSize * 0.8);
                Random tempRandom = new Random();
                // fill the state with random bytes to the fillSize
                while (fillSize > 0) {
                    byte[] randomBytes = new byte[32];
                    tempRandom.nextBytes(randomBytes);
                    currentState.add(randomBytes);
                    fillSize -= 32;
                }
                stateBytesList.update(currentState);
            }
            // add first 16 bytes of the input record to the state
            currentState.add(Arrays.copyOfRange(value.f4, 0, 16));
            stateBytesList.update(currentState);
            busyProcessing(currentNanoTime + value.f1 * 1_000_000L);
            final String keyWithSubtask = value.f0 + "_" + getRuntimeContext().getIndexOfThisSubtask();

            return new Tuple3<>(keyWithSubtask, currentState.size(), new Tuple3<>(value.f2, value.f3,System.currentTimeMillis()));
        }

        private void busyProcessing(long endNanos) {
            while (System.nanoTime() < endNanos) {
                // busy-wait
            }
        }
    }

    static class MySource extends
            RichParallelSourceFunction<Tuple5<String, Integer, Long, Long, byte[]>>
           implements CheckpointedFunction {

        private volatile boolean isRunning = true;

        private final int runtime;
        private final List<String> keyList;

        private final int[] inputRates;
        private final int recordSize;

        ProcessingTimeDistribution processingTimeDistribution;
        double processingTimeMean;
        double processingTimeStddev;

        RecordsPerKeyDistribution recordsPerKeyDistribution;
        private final double skewFactor;
        private double[] cumulativeWeights;
        private final boolean keepAliveAfterEmitComplete;


        private transient ListState<Tuple5<Map<String, Integer>, Integer, Integer, Integer, Long>> checkpointedState;
        private Map<String, Integer> keyWithCount = new HashMap<>();
        private int totalCount = 0;
        private int currSec;
        private int expectedRecords;
        private long globalStartMillTime;

        private final Random localRandom = new Random(1001);

        public MySource(int runtime,
                        List<String> keyList,
                        int rate,
                        InputDistribution inputDistribution,
                        int sourceParallelism,
                        int recordSize,
                        ProcessingTimeDistribution recordProcessingTimeDistribution,
                        double processingTimeMean,
                        double processingTimeStddev,
                        RecordsPerKeyDistribution recordsPerKeyDistribution,
                        double keySkewFactor,
                        boolean keepAliveAfterEmitComplete) {

            if (runtime < 1|| rate < 1 || recordSize < 1 || processingTimeMean < 1) {
                throw new IllegalArgumentException("All parameters must be greater than 0");
            }


            this.skewFactor = keySkewFactor;

            this.runtime=runtime;
            this.keyList = keyList;

            this.keepAliveAfterEmitComplete = keepAliveAfterEmitComplete;

            inputRates = new int[sourceParallelism];
            if (inputDistribution == InputDistribution.EVENLY) {
                int ratePerInstance = rate / sourceParallelism;
                int remainder = rate % sourceParallelism;
                for (int i = 0; i < sourceParallelism; i++) {
                    inputRates[i] = ratePerInstance + (i < remainder ? 1 : 0);
                }
            } else {
                // first instance gets 70% of the rate, the rest takes an equal share of the remaining 30%
                inputRates[0] = (int) (rate * 0.7);
                int remainingRate = rate - inputRates[0];
                int ratePerRemainingInstance = remainingRate / (sourceParallelism - 1);
                int remainder = remainingRate % (sourceParallelism - 1);
                for (int i = 1; i < sourceParallelism; i++) {
                    inputRates[i] = ratePerRemainingInstance + (i <= remainder ? 1 : 0);
                }
            }
            // check sum of input rates = rate
            if (Arrays.stream(inputRates).sum() != rate) {
                throw new IllegalArgumentException("Sum of input rates must be equal to the total rate");
            }


            if (recordsPerKeyDistribution == RecordsPerKeyDistribution.ZIPF) {
                if (keySkewFactor < 0) {
                    throw new IllegalArgumentException("Skew factor must be greater than 0 for SKEW distribution.");
                } else if (keySkewFactor == 0) {
                    LOG.warn("Skew factor is 0, falling back to EVEN distribution.");
                    recordsPerKeyDistribution = RecordsPerKeyDistribution.EVENLY;
                }else{
                    int keyCount = keyList.size();
                    cumulativeWeights = new double[keyCount];
                    double sum = 0.0;
                    for (int i = 0; i < keyCount; i++) {
                        sum += 1.0 / Math.pow(i + 1, keySkewFactor);
                        cumulativeWeights[i] = sum;
                    }
                    LOG.info("Cumulative weights: {}", Arrays.toString(cumulativeWeights));
                }
            }

            this.recordSize=recordSize;

            this.processingTimeDistribution = recordProcessingTimeDistribution;
            this.processingTimeMean = processingTimeMean;
            this.processingTimeStddev = processingTimeStddev;
            this.recordsPerKeyDistribution = recordsPerKeyDistribution;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void run(SourceContext<Tuple5<String, Integer, Long, Long, byte[]>> ctx) throws Exception {

            final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
            final int taskRate = inputRates[taskIndex];
            
            long nanosPerRecord;
            final int totalGenNum = runtime * taskRate;
            LOG.info("{}: Start emitting records: {}/{} with expectedRecords {}, current time {}/{}",
                    taskIndex, totalCount, totalGenNum, expectedRecords, currSec, runtime);


            long startNanoTime = System.nanoTime();
            if (totalCount == 0) {
                globalStartMillTime = System.currentTimeMillis();
            }else{
                // adjust currSec and expectedRecords to simulate the time passed during system halt
                int realPassedSec = (int) ((System.currentTimeMillis() - globalStartMillTime) / 1000);
                expectedRecords = Math.min(
                        (taskRate * (realPassedSec - currSec)) + expectedRecords,
                        totalGenNum - totalCount);
                currSec = realPassedSec;
                LOG.info("{}: After adjusting due to system halt: {}/{} with expectedRecords {}, current time {}/{}",
                        taskIndex, totalCount, totalGenNum, expectedRecords, currSec, runtime);
            }

            final double intervalMs = 1000.0 / taskRate;

            for (; currSec < runtime; currSec++) {
                long secStartNanoTime = startNanoTime + currSec * 1_000_000_000L;
                long secEndNanoTime = secStartNanoTime + 1_000_000_000L;

                expectedRecords += Math.min(taskRate, totalGenNum - totalCount);
                if (expectedRecords == 0) {
                    break;
                }
                nanosPerRecord = 1_000_000_000L / expectedRecords;

                int secCount = 0;

                while (isRunning && secCount < expectedRecords) {
                    long currentNanoTime = System.nanoTime();
                    if (currentNanoTime >= secEndNanoTime) {
                        break;
                    }
                    genRecord(ctx, taskIndex, intervalMs);
                    secCount++;
                    totalCount++;

                    long targetNanoTime = secStartNanoTime + secCount * nanosPerRecord;
                    long remainingNanos = targetNanoTime - System.nanoTime();
                    if (remainingNanos > 0) {
                        LockSupport.parkNanos(remainingNanos);
                    }
                }
                expectedRecords -= secCount;
                if (!isRunning) {
                    break;
                }
                long remainingNanos = secEndNanoTime - System.nanoTime();
                if (remainingNanos > 0) {
                    LockSupport.parkNanos(remainingNanos);
                }
            }
            LOG.info("{}: Finished emitting records loop: {}/{} with expectedRecords {}, "
                            + "current time {}/{}, isRunning: {}",
                    taskIndex, totalCount, totalGenNum, expectedRecords, currSec, runtime, isRunning);
            while(isRunning && expectedRecords > 0) {
                genRecord(ctx, taskIndex, intervalMs);
                expectedRecords--;
            }

            if (keepAliveAfterEmitComplete) {
                // keeps alive but not emitting records, until last 30 minutes from globalStartMillTime
                LOG.info("keepAliveAfterEmitComplete: true, will keep alive for 30 minutes after emitting complete until {}",
                        globalStartMillTime + 30 * 60 * 1000);
                while (isRunning ) {
                    LockSupport.parkNanos(1_000_000_000L);
                    // if from the start of the program, it has been running for 30 minutes
                    if (System.currentTimeMillis() - globalStartMillTime > 30 * 60 * 1000) {
                        break;
                    }
                }
            }

        }

        private void genRecord(
                SourceContext<Tuple5<String, Integer, Long, Long, byte[]>> ctx,
                int taskIndex,
                double intervalMs) {
            String key = selectKey();
            int uniqueKeyCount = keyWithCount.getOrDefault(key, 0);
            keyWithCount.put(key, uniqueKeyCount + 1);
            int processingTime = generateProcessingTime();

            String recordKey = key + "_" + uniqueKeyCount + "_" + taskIndex;
            byte[] placeholder = new byte[recordSize];
            localRandom.nextBytes(placeholder);

            Long expectedGenTime = (long)(totalCount * intervalMs) + globalStartMillTime;
            Long realGenTime = System.currentTimeMillis();

            ctx.collect(new Tuple5<>(recordKey, processingTime, expectedGenTime, realGenTime, placeholder));
        }

        private int generateProcessingTime() {
            switch (processingTimeDistribution) {
                case UNIFORM:
                    double time = processingTimeMean - processingTimeStddev + localRandom.nextDouble() * 2 * processingTimeStddev;
                    return (int) Math.max(1, time);
                case NORMAL:
                case ZIPF:
                    throw new UnsupportedOperationException("Not implemented yet");
                default:
                    throw new IllegalArgumentException("Unknown processingTimeDistribution: " + processingTimeDistribution);
            }
        }

        private String selectKey() {
            switch (recordsPerKeyDistribution) {
                case EVENLY:
                    return keyList.get(localRandom.nextInt(keyList.size()));
                case ZIPF:
                    if (cumulativeWeights == null || cumulativeWeights.length == 0) {
                        throw new IllegalStateException("Cumulative weights not initialized for ZIPF distribution.");
                    }
                    double maxWeight = cumulativeWeights[cumulativeWeights.length - 1];
                    double r = localRandom.nextDouble() * maxWeight;
                    int index = Arrays.binarySearch(cumulativeWeights, r);
                    if (index < 0) {
                        index = -index - 1;
                    }
                    index = Math.min(index, keyList.size() - 1);
                    return keyList.get(index);
                case STATE_SIZE:
                    throw new UnsupportedOperationException("Not implemented yet");
                default:
                    throw new IllegalArgumentException("Unknown recordsPerKeyDistribution: " + recordsPerKeyDistribution);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        // to support checkpoint
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            checkpointedState.add(new Tuple5<>(
                    new HashMap<>(keyWithCount),
                    totalCount,
                    currSec,
                    expectedRecords,
                    globalStartMillTime));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple5<Map<String, Integer>, Integer, Integer, Integer, Long>> descriptor =
                    new ListStateDescriptor<>(
                            "sourceState",
                            TypeInformation.of(new TypeHint<Tuple5<Map<String, Integer>, Integer, Integer, Integer, Long>>() {}));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Tuple5<Map<String, Integer>, Integer, Integer, Integer, Long> state : checkpointedState.get()) {
                    LOG.info("Restoring state: {}", state);
                    this.keyWithCount = state.f0;
                    this.totalCount = state.f1;
                    this.currSec = state.f2;
                    this.expectedRecords = state.f3;
                    this.globalStartMillTime = state.f4;
                }
            } else {
                this.keyWithCount = new HashMap<>();
                this.totalCount = 0;
                this.currSec = 0;
                this.expectedRecords = 0;
            }
        }
    }
}
