package com.github.gquintana.elasticsearch;

import com.beust.jcommander.JCommander;
import com.github.gquintana.elasticsearch.index.IndexCommand;
import com.github.gquintana.elasticsearch.search.SearchCommand;

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
        Command command = null;
        if (commander.getParsedCommand().equals("index")) {
            command = indexCommand;
        } else if (commander.getParsedCommand().equals("search")) {
            command = searchCommand;
        } else {
            commander.usage("index");
            commander.usage("search");
        }
        if (command !=null) {
            registerShutdownHook(command);
            command.execute();
        }
    }
    private static void registerShutdownHook(final Command command) {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                command.close();
            }
        });
    }
}
