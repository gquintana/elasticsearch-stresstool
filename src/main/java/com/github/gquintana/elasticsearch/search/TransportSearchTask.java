package com.github.gquintana.elasticsearch.search;

import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

/**
 * Service to index using transport protocol
 */
public class TransportSearchTask extends SearchTask {
    private final Client client;

    public TransportSearchTask(Client client, DataProvider dataProvider, TemplatingService templatingService) {
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
