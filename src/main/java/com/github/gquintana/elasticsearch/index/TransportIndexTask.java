package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.EsStressToolException;
import com.github.gquintana.elasticsearch.Resources;
import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;

import java.io.IOException;

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

    @Override
    public void prepare() {
        IndicesAdminClient indices = client.admin().indices();
        if (indexName != null) {
            boolean indexExists = indices.prepareExists(indexName).execute().actionGet().isExists();
            if (indexExists && indexDelete) {
                indices.prepareDelete(indexName).execute().actionGet();
                indexExists = false;
            }
            if (!indexExists) {
                CreateIndexRequestBuilder createIndexRequest = indices.prepareCreate(indexName);
                if (indexSettings != null) {
                    try {
                        byte[] source = Resources.loadBytes(indexSettings);
                        createIndexRequest.setSource(source);
                    } catch (IOException e) {
                        throw new EsStressToolException("Index creation failed", e);
                    }
                }
                createIndexRequest.execute().actionGet();
            }
        }
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
