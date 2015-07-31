package com.github.gquintana.elasticsearch;

import org.elasticsearch.client.Client;

public class SearchCommand extends Command {

    @Override
    protected Task createTask() {
        Client client = createTransportClient();
        DataProvider dataProvider = createDataProvider();
        TemplatingService templatingService = new TemplatingService();
        TransportSearchingTask task = new TransportSearchingTask(client, dataProvider, templatingService);
        if (docTemplate != null) task.setTemplateLocation(docTemplate);
        return task;
    }
}
