package com.github.gquintana.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

/**
 * Service to index using transport protocol
 */
public class TransportSearchingTask extends Task {
    private final Client client;

    public TransportSearchingTask(Client client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    public void execute() {
        Data data = provideData();
        SearchRequestBuilder searchRequest = client.prepareSearch(data.getIndices())
            .setTypes(data.getTypes()).setSource(data.getSource());
        SearchResponse searchResponse = searchRequest.execute().actionGet();
    }

    public Client getClient() {
        return client;
    }
}
