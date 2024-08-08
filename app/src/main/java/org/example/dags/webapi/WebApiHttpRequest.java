package org.example.dags.webapi;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class WebApiHttpRequest implements Serializable {
    static Builder builder() {
        return new AutoValue_WebApiHttpRequest.Builder();
    }

    public static WebApiHttpRequest of(String url) {
        return builder()
                .setUrl(url)
                .setHeaders(new HashMap<String, String>())
                .build();
    }

    public static WebApiHttpRequest of(String url, Map<String, String> headers) {
        return builder()
                .setUrl(url)
                .setHeaders(headers)
                .build();
    }

    abstract String getUrl();
    abstract Map<String, String> getHeaders();

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setUrl(String url);
        abstract Builder setHeaders(Map<String, String> headers);

        abstract WebApiHttpRequest build();
    }
}
