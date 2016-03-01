package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.*;
import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Service to index using HTTP protocol and Jest
 */
public class JestIndexTask extends IndexTask {
    private final JestClient client;

    public JestIndexTask(JestClient client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    @Override
    public void prepare() {
        JestResult result;
        if (indexName != null) {
            boolean indexExists = true;
            try {
                result = execute(new IndicesExists.Builder(indexName).build(), "Index exist");
            } catch (JestResultException e) {
                indexExists = e.getResult().isSucceeded();
            }
            if (indexExists && indexDelete) {
                try {
                    result = execute(new DeleteIndex.Builder(indexName).build(), "Delete index");
                    indexExists = false;
                } catch (JestResultException e) {
                }
            }
            if (!indexExists) {
                CreateIndex.Builder createIndex = new CreateIndex.Builder(indexName);
                if (indexSettings != null) {
                    try {
                        try (InputStream indexSettingsIS = Resources.open(indexSettings)) {
                            Map<String, Object> indexSettingsJson = Jsons.parseMap(indexSettingsIS);
                            createIndex.settings(indexSettingsJson);
                        }
                    } catch (IOException e) {
                        throw new EsStressToolException("Index creation failed", e);
                    }
                }
                result = execute(createIndex.build(), "Create Index");
            }
        }
        super.prepare();
    }

    private Index prepareIndex() {
        Data data = provideData();
        return new Index.Builder(data.getSource())
                .index(data.getIndex())
                .type(data.getType()).id(data.getId())
                .build();
    }

    public void execute() {
        Action<? extends JestResult> action;
        if (shouldBulkIndex()) {
            Bulk.Builder bulkBuilder = new Bulk.Builder();
            for (int i = 0; i < bulkSize; i++) {
                bulkBuilder.addAction(prepareIndex());
            }
            action = bulkBuilder.build();
        } else {
            action = prepareIndex();
        }
        JestResult indexResult = execute(action, "Index");
    }

    private <T extends JestResult> T execute(Action<T> action, String actionName) {
        return Jests.execute(client, action, actionName);
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    public JestClient getClient() {
        return client;
    }
}
