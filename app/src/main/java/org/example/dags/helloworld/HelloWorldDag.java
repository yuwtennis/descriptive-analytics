package org.example.dags.helloworld;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.example.dags.Dag;

public class HelloWorldDag implements Dag {

    public void process(Pipeline p) {
        p.apply(Create.of("Hello", "World", "!"))
                .apply(new HelloWorldVertices.SimpleVertex());
        p.run().waitUntilFinish();
    }
}