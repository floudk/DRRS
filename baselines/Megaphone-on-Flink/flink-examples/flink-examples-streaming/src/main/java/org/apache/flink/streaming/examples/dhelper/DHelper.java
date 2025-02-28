package org.apache.flink.streaming.examples.dhelper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// floudk-add: A class to check the correctness of the control plane.
// 1. it needs to run awhile and try to add parallelism as stateless job.
public class DHelper {
    public static void main(String[] args) throws Exception{
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ban the chain optimization
        env.disableOperatorChaining();

        // Source Operator (parallelism = 1),
        // input message 'msg' to following operator 1 item/s and running for 6s
        DataStream<String> source = env.addSource(new MySource(30,10)).setParallelism(1).name("source");

        // Append Operator (parallelism = 2)
        // append a string to show the index of the instance of the operator
        DataStream<String> append = source.map(new AppendInstanceIndexMapper()).setParallelism(2).name("append");

        //Print Operator (parallelism = 1)
        // print the message from append operator to stdout
        DataStreamSink<String> print = append.print().setParallelism(1).name("print");

        // execute the program
        env.execute("Nice Try!");
    }

    public static class MySource implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private final int rate;
        private final int runtime;
        int countMsg = 1;

        public MySource(int rate,int runtime){
            this.rate=rate;
            this.runtime=runtime;
        }
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            while (isRunning && (System.currentTimeMillis() - startTime) < (runtime * 1000L)) {
                ctx.collect("msg-" + countMsg++);
                Thread.sleep((long) (1000 / rate));
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class AppendInstanceIndexMapper extends RichMapFunction<String,String> {
        private static final Logger LOG = LoggerFactory.getLogger(AppendInstanceIndexMapper.class);
        @Override
        public String map(String value) throws Exception {
            LOG.info("map: " + value + " (instance-" + getRuntimeContext().getIndexOfThisSubtask()+")");
            return value + " (instance-" + getRuntimeContext().getIndexOfThisSubtask()+")";
        }
    }
}
