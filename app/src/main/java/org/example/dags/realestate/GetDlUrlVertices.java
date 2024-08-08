package org.example.dags.realestate;

import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.Utils;
import org.example.dags.webapi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GetDlUrlVertices {
    static Logger LOG = LoggerFactory.getLogger(GetDlUrlVertices.class);

    static class ParseDlUrlFn
            extends SimpleFunction<KV<String, WebApiHttpResponse>, String> {
        @Override
        public String apply(KV<String, WebApiHttpResponse> input) {
            String result = new String(input.getValue().getData());
            LOG.info("Retrieved data: {}", result);
            Map<String, String> map = Utils.asJsonMap(result);
            return map.get("url");
        }
    }

    public static class DownloadUrl
            extends PTransform<PCollection<String>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            LOG.info("Start downloading url");
            Map<String, String> headers = new HashMap<String, String>();
            headers.put(OcpApimSubscriptionKeyHeader.NAME, OcpApimSubscriptionKeyHeader.VALUE);

            KvCoder<String, WebApiHttpResponse> respCoder = KvCoder.of(
                    StringUtf8Coder.of(), WebApiHttpResponseCoder.of());

            PCollection<WebApiHttpRequest> requests = input.apply(
                            MapElements
                                    .into(TypeDescriptor.of(WebApiHttpRequest.class))
                                    .via((String url) -> WebApiHttpRequest.of(url, headers)))
                    .setCoder(WebApiHttpRequestCoder.of());

            Result<KV<String, WebApiHttpResponse>> results = requests
                    .apply(RequestResponseIO.of(WebApiHttpClient.of(), respCoder));

            return results.getResponses()
                    .apply(MapElements.via(new GetDlUrlVertices.ParseDlUrlFn()));
        }
    }
}