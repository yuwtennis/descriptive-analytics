package org.example.dags.realestate;

import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.dags.webapi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ExtractZipContentsVertices {
    static Logger LOG = LoggerFactory.getLogger(ExtractZipContentsVertices.class);

    static class UnzipFn extends DoFn<KV<String, WebApiHttpResponse>, RealEstatesXactRec> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException, Exception {
            KV<String, WebApiHttpResponse> elem = c.element();
            ByteArrayInputStream bis = new ByteArrayInputStream(elem.getValue().getData());
            ZipInputStream zs = new ZipInputStream(bis);
            // TODO Should iterate through zip file for expected file
            zs.getNextEntry();

            BufferedReader br = new BufferedReader(
                    new InputStreamReader(zs, Charset.forName("windows-31j")));

            while (br.ready()) {
                String line = br.readLine();
                if (line.startsWith("\"種類")) {
                    continue;
                }
                c.output(RealEstatesXactRec.of(line));
            }

            zs.close();
            bis.close();
            br.close();
         }
    }

    public static class Extract extends PTransform<PCollection<String>, PCollection<RealEstatesXactRec>> {
        @Override
        public PCollection<RealEstatesXactRec> expand(PCollection<String> input) {
            LOG.info("Start extracting zip file contents");
            KvCoder<String, WebApiHttpResponse> respCoder = KvCoder.of(
                    StringUtf8Coder.of(), WebApiHttpResponseCoder.of());

            PCollection<WebApiHttpRequest> requests = input
                    .apply(
                            MapElements
                                    .into(TypeDescriptor.of(WebApiHttpRequest.class))
                                    .via(WebApiHttpRequest::of))
                    .setCoder(WebApiHttpRequestCoder.of());

            Result<KV<String, WebApiHttpResponse>> results = requests
                    .apply(RequestResponseIO.of(WebApiHttpClient.of(), respCoder));

            return results
                    .getResponses()
                    .apply(ParDo.of(new ExtractZipContentsVertices.UnzipFn()));
        }
    }
}
