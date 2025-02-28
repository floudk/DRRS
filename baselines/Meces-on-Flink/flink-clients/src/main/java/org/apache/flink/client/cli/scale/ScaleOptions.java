package org.apache.flink.client.cli.scale;

import org.apache.commons.cli.CommandLine;

import org.apache.flink.client.cli.CommandLineOptions;

public class ScaleOptions extends CommandLineOptions {

    private final String[] args;

    public ScaleOptions(CommandLine line){
        super(line);
        this.args = line.getArgs();
    }

    public String[] getArgs() {
        return args;
    }

}
