package org.apache.flink.streaming.examples.nexmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Nexmark {
    public final static Logger LOG = LoggerFactory.getLogger(Nexmark.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Nexmark started with args: {}", String.join(" ", args));
        NexmarkCLI cli = NexmarkCLI.fromArgs(args);
        executeQuery(cli);
    }

    private static void executeQuery(NexmarkCLI cli) throws Exception{
        if (cli.query.equals("q7")){
            Query7 q7 = new Query7();
            q7.run(cli);
        }else if(cli.query.equals("q8")){
            Query8 q8 = new Query8();
            q8.run(cli);
        }
    }
}
