package com.github.gquintana.elasticsearch;

import com.beust.jcommander.JCommander;

/**
 * Main class
 */
public class EsStressTool {
    public static void main(String[] args) {
        JCommander commander = new JCommander();
        commander.setProgramName("esstresstool");
        IndexCommand indexCommand = new IndexCommand();
        commander.addCommand("index", indexCommand);
        SearchCommand searchCommand = new SearchCommand();
        commander.addCommand("search", searchCommand);
        commander.addCommand("help", new Object());
        commander.parse(args);
        if (commander.getParsedCommand().equals("index")) {
            indexCommand.execute();
        } else if (commander.getParsedCommand().equals("search")) {
            searchCommand.execute();
        } else {
            commander.usage("index");
            commander.usage("search");
        }
    }
}
