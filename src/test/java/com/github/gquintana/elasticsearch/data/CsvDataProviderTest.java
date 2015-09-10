package com.github.gquintana.elasticsearch.data;

import com.github.gquintana.elasticsearch.Resources;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CsvDataProviderTest {
    private void assertDataEquals(String index, String type, Integer id, String title, Data data) {
        assertEquals(index, data.getIndex());
        assertEquals(type, data.getType());
        if (id == null) {
            assertNull(data.getId());
        } else {
            assertEquals(id, Integer.valueOf(data.getId()));
        }
        assertEquals(title, data.getSourceAt("title"));
    }

    @Test
    public void testProvideWithColumns() {
        // Given
        CsvDataProvider provider = new CsvDataProvider(Resources.classResource(getClass(), "CsvDataProviderTest.csv"));
        // When
        Data d1 = provider.provide();
        Data d2 = provider.provide();
        Data d3 = provider.provide();
        Data d4 = provider.provide();
        // Then
        assertDataEquals("test_index", "test_type_1", 1, "One", d1);
        assertDataEquals("test_index", "test_type_1", 2, "Two", d2);
        assertDataEquals("test_index", "test_type_2", 1, "Three", d3);
        assertDataEquals("test_index", "test_type_1", 1, "One", d4);
    }
    @Test
    public void testProvideWithoutColumns() {
        // Given
        CsvDataProvider provider = new CsvDataProvider(Resources.classResource(getClass(),"CsvDataProviderTest.csv"));
        provider.setIndexColumn("xIndex");
        provider.setTypeColumn("xType");
        provider.setIdColumn("xId");
        // When
        Data d1 = provider.provide();
        Data d2 = provider.provide();
        Data d3 = provider.provide();
        Data d4 = provider.provide();
        // Then
        assertDataEquals("index", "type", null, "One", d1);
        assertDataEquals("index", "type", null, "Two", d2);
        assertDataEquals("index", "type", null, "Three", d3);
        assertDataEquals("index", "type", null, "One", d4);
    }

}