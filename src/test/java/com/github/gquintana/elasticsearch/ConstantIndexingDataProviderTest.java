package com.github.gquintana.elasticsearch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConstantIndexingDataProviderTest {
    private void assertDataEquals(String index, String type, Data data) {
        assertEquals(index, data.getIndex());
        assertEquals(type, data.getType());
    }

    @Test
    public void testProvide() {
        // Given
        ConstantDataProvider provider = new ConstantDataProvider("index","type");
        // When
        Data d1 = provider.provide();
        Data d2 = provider.provide();
        Data d3 = provider.provide();
        Data d4 = provider.provide();
        // Then
        assertDataEquals("index", "type", d1);
        assertDataEquals("index", "type", d2);
        assertDataEquals("index", "type", d3);
    }

    @Test
    public void testProvideForTemplate() {
        // Given
        ConstantDataProvider provider = new ConstantDataProvider("index","type");
        TemplatingService templatingService = new TemplatingService();
        // When
        Data d1 = provider.provide(templatingService, Resources.classResource(getClass(), getClass().getSimpleName() + ".mustache"));
        // Then
        assertDataEquals("index", "type", d1);
        assertNotNull(d1.getSourceAt("randomInt"));
        assertNotNull(d1.getSourceAt("randomBoolean"));
        assertNotNull(d1.getSourceAt("timestamp"));
        assertNotNull(d1.getSourceAt("number"));
    }

}