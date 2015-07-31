package com.github.gquintana.elasticsearch;

import com.beust.jcommander.Parameter;
import org.elasticsearch.client.Client;

public class IndexCommand extends Command {
    @Parameter(names = {"-b", "--bulk-size"}, description = "Bulk size")
    private Integer bulkSize;

    @Override
    protected Task createTask() {
        Client client = createTransportClient();
        DataProvider dataProvider = createDataProvider();
        TemplatingService templatingService = new TemplatingService();
        TransportIndexingTask task = new TransportIndexingTask(client, dataProvider, templatingService);
        if (bulkSize != null) task.setBulkSize(bulkSize);
        if (docTemplate != null) task.setTemplateLocation(docTemplate);
        return task;
    }
}
