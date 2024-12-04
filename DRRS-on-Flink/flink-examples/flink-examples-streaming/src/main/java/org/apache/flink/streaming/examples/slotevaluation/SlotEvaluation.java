package org.apache.flink.streaming.examples.slotevaluation;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by floudk on 24/11/2023
 * Job to evaluate the performance of the slot allocation.
 * Four operator are used: A, B, C, D.
 * A receives a stream of data from socket and forwards it to B.
 * B is kind of CPU-intensive and forwards the data to C.
 * C shuffles the data and forwards it to D.
 * D is kind of IO-intensive and writes the data to a file.
 */

public class SlotEvaluation {

    private static final Logger LOG = LoggerFactory.getLogger(SlotEvaluation.class);

    static final String hostname = "input";
    static final int port = 9999;

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        int parallelism = 1;
        String slotGroupA = "1";
        String slotGroupB = "1";
        String slotGroupC = "1";
        String slotGroupD = "1";

        // get the case id [1,2,3,4].
        // by default, case 1 is used.
        int caseId = (args.length > 0) ? Integer.parseInt(args[0]) : 1;
        if (args.length > 0) {
            LOG.info("Received parameters: ");
            LOG.info(Arrays.toString(args));
        }else{
            LOG.info("No parameters received. Using default case id: " + caseId);
        }

        LOG.info("Current case id: " + caseId);
        switch (caseId) {
            case 1:
                // case 1: parallelism = 1 and only one slot
                // A -> B -> C -> D
                break;
            case 2:
                // case 2: parallelism = 1 and 4 slots, each slot owns one operator
                // A -> B -> C -> D
                slotGroupA = "1";
                slotGroupB = "2";
                slotGroupC = "3";
                slotGroupD = "4";
                break;
            case 3:
                // case 3: parallelism = 4 and 4 slots, each slot owns all instances of operator
                // A -> B -> C -> D
                parallelism = 4;
                break;
            case 4:
                // case 4: parallelism = 8 and 8 slots, each slot owns 1 instances of each operator
                // A -> B -> C -> D
                parallelism = 8;
                break;
            default:
                throw new IllegalArgumentException("Invalid case id: " + caseId);
        }


        // Operator A: socket source
        DataStream<String> source = env.addSource(new MySocketSource(hostname,port))
                .setParallelism(1)
                .slotSharingGroup(slotGroupA)
                .name("socketSource");


        DataStream<Tuple2<Long,Long>> transfer =  source.map(value -> {
                    String[] parts = value.split(",");
                    return new Tuple2<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
                }).returns(Types.TUPLE(Types.LONG, Types.LONG)).name("map")
                .setParallelism(parallelism)
                .slotSharingGroup(slotGroupB);

        // Operator B: CPU-intensive
        DataStream<Tuple2<Long,String>> cpuIntensive = transfer.map(new CPUIntensiveOp()).name("cpuIntensive")
                .setParallelism(parallelism)
                .slotSharingGroup(slotGroupC);
        
        // Operator C: Shuffle operation

        final int finalParallelism = parallelism;

        KeyedStream<Tuple2<Long, String>, Long> keyedStream = cpuIntensive
                .keyBy((KeySelector<Tuple2<Long, String>, Long>) value -> {
                    return value.f0 % finalParallelism;  // Key by the first field's modulo
                });

        // Operator D: IO-intensive
        keyedStream.addSink(new IOIntensiveOp(caseId))
                .setParallelism(parallelism)
                .slotSharingGroup(slotGroupD);


        env.execute("Evaluate Slot Allocation"); 

    }


    public static class CPUIntensiveOp extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

        @Override
        public Tuple2<Long, String> map(Tuple2<Long, Long> value) throws Exception {
            long number = value.f0;
            long fibonacciResult = fibonacci(number);
            String subtaskIndex = String.valueOf(getRuntimeContext().getIndexOfThisSubtask());

            return new Tuple2<>(value.f1, subtaskIndex);
        }

        private long fibonacci(long n) {
            if (n <= 1)
                return n;
            else
                return fibonacci(n - 1) + fibonacci(n - 2);
        }
    }


    public static class MySocketSource extends SocketTextStreamFunction {

        private Boolean endOfStream = false;

        public MySocketSource(String hostname, int port) {
            super(hostname, port, "\n", Long.MAX_VALUE);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            final StringBuilder buffer = new StringBuilder();
            long attempt = 0;

            while (isRunning&&!endOfStream) {
                try (Socket socket = new Socket()) {
                    currentSocket = socket;

                    LOG.info("Connecting to server socket " + hostname + ':' + port);
                    socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
                    try (BufferedReader reader =
                                 new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                        char[] cbuf = new char[8192];
                        int bytesRead;
                        while (!endOfStream&&isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                            buffer.append(cbuf, 0, bytesRead);
                            int delimPos;
                            while (!endOfStream&&buffer.length() >= delimiter.length()
                                    && (delimPos = buffer.indexOf(delimiter)) != -1) {
                                String record = buffer.substring(0, delimPos);
                                // truncate trailing carriage return
                                if (delimiter.equals("\n") && record.endsWith("\r")) {
                                    record = record.substring(0, record.length() - 1);
                                }
                                LOG.info("Received message: " + record);
                                if(record.equals("endOfStream")){
                                    endOfStream = true;
                                    buffer.delete(0, delimPos + delimiter.length());
                                    break;
                                }
                                ctx.collect(record);
                                buffer.delete(0, delimPos + delimiter.length());
                            }
                        }
                    }
                }

                // if we dropped out of this loop due to an EOF, sleep and retry
                if (isRunning&&!endOfStream) {
                    attempt++;
                    if (maxNumRetries == -1 || attempt < maxNumRetries) {
                        LOG.warn(
                                "Lost connection to server socket. Retrying in "
                                        + delayBetweenRetries
                                        + " msecs...");
                        Thread.sleep(delayBetweenRetries);
                    } else {
                        // this should probably be here, but some examples expect simple exists of the
                        // stream source
                        // throw new EOFException("Reached end of stream and reconnects are not
                        // enabled.");
                        break;
                    }
                }
            }

            // collect trailing data
            if (buffer.length() > 0) {
                ctx.collect(buffer.toString());
            }

        }
    }

    public static class IOIntensiveOp extends RichSinkFunction<Tuple2<Long, String>> {
        private transient BufferedWriter writer;
        private final String pathPrefix;

        IOIntensiveOp(int caseId){
            this.pathPrefix ="/opt/output/"+ caseId +"/io-intensive-";
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            String subtaskIndex = String.valueOf(getRuntimeContext().getIndexOfThisSubtask());
            File file = new File(pathPrefix + subtaskIndex + ".txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));
        }

        @Override
        public void invoke(Tuple2<Long, String> value, Context context) throws IOException {

            // Assuming value.f1 is the length of data to write
            writer.write("cpuIntensiveOp: " + value.f1 + " - ioIntensiveOp: " +
                    getRuntimeContext().getIndexOfThisSubtask() + ": writing " + value.f0 + " bytes");
            writer.newLine();
            for (int i = 0; i < value.f0; i++) {
                writer.write("a");
            }
            writer.newLine();
            writer.flush();  // Ensure all data is written to the file
        }

        @Override
        public void close() throws Exception {
            super.close();
            writer.close();
        }
    }
}
