package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.EmbeddedElasticsearch;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class IndexTaskTest {
	public static final String TEST_INDEX = "testindex";

	@BeforeClass
	public static void setUpClass() {
		EmbeddedElasticsearch.node();
	}

	protected abstract IndexTask createTask() ;

	@Test
	public void testPrepare_Create() {
		// Given
		EmbeddedElasticsearch.delete(TEST_INDEX);
		IndexTask task = createTask();
		task.setIndexName(TEST_INDEX);
		task.setIndexSettings("classpath:com/github/gquintana/elasticsearch/index/index-settings.json");
		// When
		task.prepare();
		// Then
		GetSettingsResponse getSettingsResponse = EmbeddedElasticsearch.client().admin().indices()
				.prepareGetSettings(TEST_INDEX).execute().actionGet();
		Settings indexSettings = getSettingsResponse.getIndexToSettings().get(TEST_INDEX);
		assertEquals(1, indexSettings.getAsInt("index.number_of_shards", null).intValue());
		assertEquals(0, indexSettings.getAsInt("index.number_of_replicas", null).intValue());
		assertEquals("30s", indexSettings.get("index.refresh_interval", null));
		EmbeddedElasticsearch.delete(TEST_INDEX);
	}

	@Test
	public void testPrepare_DeleteCreate() {
		// Given
		IndexTask task = createTask();
		task.setIndexName(TEST_INDEX);
		task.prepare();
		task.setIndexSettings("classpath:com/github/gquintana/elasticsearch/index/index-settings.json");
		task.setIndexDelete(true);
		// When
		task.prepare();
		// Then
		GetSettingsResponse getSettingsResponse = EmbeddedElasticsearch.client().admin().indices()
				.prepareGetSettings(TEST_INDEX).execute().actionGet();
		Settings indexSettings = getSettingsResponse.getIndexToSettings().get(TEST_INDEX);
		assertEquals(1, indexSettings.getAsInt("index.number_of_shards", null).intValue());
		assertEquals(0, indexSettings.getAsInt("index.number_of_replicas", null).intValue());
		assertEquals("30s", indexSettings.get("index.refresh_interval", null));
		EmbeddedElasticsearch.delete(TEST_INDEX);
	}

	@Test
	public void testPrepare_CreateTwice() {
		// Given
		IndexTask task = createTask();
		task.setIndexName(TEST_INDEX);
		task.prepare();
		task.setIndexSettings("classpath:com/github/gquintana/elasticsearch/index/index-settings.json");
		// When
		task.prepare();
		// Then
		GetSettingsResponse getSettingsResponse = EmbeddedElasticsearch.client().admin().indices()
				.prepareGetSettings(TEST_INDEX).execute().actionGet();
		Settings indexSettings = getSettingsResponse.getIndexToSettings().get(TEST_INDEX);
		assertEquals(5, indexSettings.getAsInt("index.number_of_shards", null).intValue());
		assertEquals(1, indexSettings.getAsInt("index.number_of_replicas", null).intValue());
		EmbeddedElasticsearch.delete(TEST_INDEX);
	}

	@Test
	public void testExecuteBulk() {
		// Given
		IndexTask service = createTask();
		service.setBulkSize(10);
		// When
		service.execute();
		// Then
		EmbeddedElasticsearch.refresh("index");
		assertEquals(10L, EmbeddedElasticsearch.count("index"));
		EmbeddedElasticsearch.delete("index");
	}

	@Test
	public void testExecuteOne() {
		// Given
		IndexTask service = createTask();
		// When
		service.execute();
		// Then
		EmbeddedElasticsearch.refresh("index");
		assertEquals(1L, EmbeddedElasticsearch.count("index"));
		EmbeddedElasticsearch.delete("index");
	}

	@AfterClass
	public static void tearDownClass() {
		EmbeddedElasticsearch.close();
	}

}
