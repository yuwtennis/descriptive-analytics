package org.example.dags.webapi;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.beam.io.requestresponse.Caller;
import org.apache.beam.io.requestresponse.UserCodeExecutionException;
import org.apache.beam.sdk.values.KV;
import org.example.dags.realestate.OcpApimSubscriptionKeyHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class WebApiHttpClient
        implements Caller<WebApiHttpRequest, KV<String, WebApiHttpResponse>> {

    static Logger LOG = LoggerFactory.getLogger(WebApiHttpClient.class);
    private static final HttpRequestFactory REQUEST_FACTORY =
            new NetHttpTransport().createRequestFactory();
    public static WebApiHttpClient of() {
        return new WebApiHttpClient();
    }

    @Override
    public KV<String, WebApiHttpResponse> call(WebApiHttpRequest webApiHttpRequest)
            throws UserCodeExecutionException {
        try {
            GenericUrl url = new GenericUrl(webApiHttpRequest.getUrl());
            HttpRequest request = REQUEST_FACTORY
                    .buildGetRequest(url);

            if(! webApiHttpRequest.getHeaders().isEmpty()) {
                request.setHeaders(createHeaders(webApiHttpRequest.getHeaders()));
            }

            HttpResponse response = request.execute();

            if(! response.isSuccessStatusCode()) {
                throw new RuntimeException("HTTP Status Code: " + response.getStatusCode());
            }

            InputStream is = response.getContent();

            return KV.of(
              webApiHttpRequest.getUrl(),
              WebApiHttpResponse
                      .builder()
                      .setData(is.readAllBytes())
              .build()
            );

        } catch (IOException | RuntimeException e) {
            throw new UserCodeExecutionException(e);
        }
    }

    /***
     *
     * @param headers
     * @return
     */
    private HttpHeaders createHeaders(Map<String, String> headers)
            throws RuntimeException{
        HttpHeaders httpHeaders = new HttpHeaders();

        LOG.info("Entires headers: {}", headers.toString());

        for (Map.Entry<String, String> header : headers.entrySet()) {
            httpHeaders.set(header.getKey(), header.getValue());
        }

        return httpHeaders;
    }
}
