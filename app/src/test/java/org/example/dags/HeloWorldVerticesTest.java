package org.example.dags;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.example.dags.helloworld.HelloWorldVertices;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HeloWorldVerticesTest {

    private List<String> input;

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
        input = new ArrayList<String>(Arrays.asList("Hello", "World."));
    }

    @Test
    @Category(NeedsRunner.class)
    public void testStringForwardFn() {
        PCollection<String> pCol = p.apply(Create.of(input))
                .apply(MapElements.via(new HelloWorldVertices.StringForwardFn()));
        PAssert.that(pCol).containsInAnyOrder(input);
        p.run().waitUntilFinish();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testSimpleVertex() {
        PCollection<String> pCol = p.apply(Create.of(input))
                .apply(new HelloWorldVertices.SimpleVertex());
        PAssert.that(pCol).containsInAnyOrder(input);
        p.run().waitUntilFinish();
    }
}
