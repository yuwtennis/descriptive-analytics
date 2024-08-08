package org.example.dags.webapi;

import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class WebApiHttpRequestCoder extends CustomCoder<WebApiHttpRequest> {
    private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
    private static final MapCoder<String, String> MAP_CODER = MapCoder.of(
            STRING_UTF_8_CODER, STRING_UTF_8_CODER);

    public static WebApiHttpRequestCoder of() {
        return new WebApiHttpRequestCoder();
    }

    @Override
    public void encode(WebApiHttpRequest value, OutputStream outputStream) throws IOException {
        STRING_UTF_8_CODER.encode(value.getUrl(), outputStream);
        MAP_CODER.encode(value.getHeaders(), outputStream);
    }

    @Override
    public WebApiHttpRequest decode(InputStream inputStream) throws IOException {
        String url = STRING_UTF_8_CODER.decode(inputStream);
        Map<String, String> headers = MAP_CODER.decode(inputStream);
        return WebApiHttpRequest.builder().setUrl(url).setHeaders(headers).build();
    }
}
