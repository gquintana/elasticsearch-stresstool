package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

/**
 * Service to index using transport protocol
 */
public class TransportIndexTask extends IndexTask {
    private final Client client;

    public TransportIndexTask(Client client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    private IndexRequestBuilder prepareIndex() {
        Data data = provideData();
        return client.prepareIndex(data.getIndex(), data.getType(), data.getId())
                .setSource(data.getSource());
    }

    public void execute() {
        if (shouldBulkIndex()) {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (int i = 0; i < bulkSize; i++) {
                bulkRequestBuilder.add(prepareIndex());
            }
            bulkRequestBuilder.execute().actionGet();
        } else {
            prepareIndex().execute().actionGet();
        }
    }

    public Client getClient() {
        return client;
    }
}
