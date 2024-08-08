package org.example.dags.helloworld;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.example.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldVertices {
    static Logger LOG = LoggerFactory.getLogger(HelloWorldDag.class);

    public static class StringForwardFn extends SimpleFunction<String, String> {
        @Override
        public String apply(String input) {
            if (Utils.isNullOrEmpty(input)) {
                throw new NullPointerException();
            }
            LOG.info("Sending {}", input);
            return input;
        }
    }
    public static class SimpleVertex extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> pCol) throws NullPointerException {
            return pCol.apply(MapElements.via(new StringForwardFn()));
        }
    }
}
