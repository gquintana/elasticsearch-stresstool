package com.github.gquintana.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

/**
 * Service to index using transport protocol
 */
public class TransportIndexingTask extends Task {
    private final Client client;
    private int bulkSize;

    public TransportIndexingTask(Client client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    private IndexRequestBuilder prepareIndex() {
        Data data = provideData();
        return client.prepareIndex(data.getIndex(), data.getType(), data.getId())
                .setSource(data.getSource());
    }

    public void execute() {
        if (bulkSize < 2) {
            prepareIndex().execute().actionGet();
        } else {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (int i = 0; i < bulkSize; i++) {
                bulkRequestBuilder.add(prepareIndex());
            }
            bulkRequestBuilder.execute().actionGet();
        }
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    public Client getClient() {
        return client;
    }
}
