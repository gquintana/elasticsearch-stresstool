package com.github.gquintana.elasticsearch.data;

import com.github.gquintana.elasticsearch.Resources;
import org.elasticsearch.common.mustache.DefaultMustacheFactory;
import org.elasticsearch.common.mustache.Mustache;
import org.elasticsearch.common.mustache.MustacheFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Mustache templating service
 */
public class TemplatingService {
    private MustacheFactory mustacheFactory = new DefaultMustacheFactory();
    private Map<String,Mustache> mustacheTemplates = new HashMap<String, Mustache>();
    private Charset charset = Charset.defaultCharset();

    /**
     * Load and compile Mustache template
     */
    private Mustache getTemplate(String templateLocation) throws  IOException{
        Mustache mustacheTemplate = mustacheTemplates.get(templateLocation);
        if (mustacheTemplate == null) {
            try(Reader templateReader = new InputStreamReader(Resources.open(templateLocation), charset)) {
                mustacheTemplate = mustacheFactory.compile(templateReader, templateLocation);
                mustacheTemplates.put(templateLocation, mustacheTemplate);
            }
        }
        return mustacheTemplate;
    }

    /**
     * Execute Mustache template
     */
    public void render(String templateLocation, Map<String, Object> templateParameter, OutputStream outputStream) throws IOException {
        try(Writer writer = new OutputStreamWriter(outputStream, charset)) {
            getTemplate(templateLocation).execute(writer, templateParameter);
            writer.flush();
        }
    }

    /**
     * Execute Mustache template
     */
    public byte[] render(String templateLocation, Map<String, Object> templateParameter) throws IOException {
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            render(templateLocation, templateParameter, outputStream);
            return outputStream.toByteArray();
        }
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }
}
