package com.github.gquintana.elasticsearch.data;

import com.github.gquintana.elasticsearch.Resources;
import com.google.common.io.ByteStreams;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TemplatingServiceTest {
    private TemplatingService service = new TemplatingService();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Test
    public void testRenderClasspath() throws Exception {
        // Given
        Map<String, Object> templateParameters = new HashMap<>();
        templateParameters.put("id",123);
        templateParameters.put("title", "testRenderClasspath");
        // When
        byte[] bytes = service.render(Resources.classResource(getClass(), "TemplatingServiceTest.mustache"), templateParameters);
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(bytes).map();
        // Then
        assertEquals(123, map.get("id"));
        assertEquals("testRenderClasspath", map.get("title"));
    }
    @Test
    public void testRenderFile() throws Exception {
        // Given
        Map<String, Object> templateParameters = new HashMap<>();
        templateParameters.put("id",123);
        templateParameters.put("title", "testRenderClasspath");
        File templateFile =temporaryFolder.newFile("template.mustache");
        try(FileOutputStream fileOutputStream = new FileOutputStream(templateFile)) {
            ByteStreams.copy(getClass().getResourceAsStream("TemplatingServiceTest.mustache"), fileOutputStream);
        }
        // When
        byte[] bytes = service.render(templateFile.getAbsolutePath(), templateParameters);
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(bytes).map();
        // Then
        assertEquals(123, map.get("id"));
        assertEquals("testRenderClasspath", map.get("title"));
    }
}