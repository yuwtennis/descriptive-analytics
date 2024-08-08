package org.example.dags;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.example.App;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.example.Utils.asJsonStr;
import static org.junit.Assert.assertTrue;

public class HelloWorldDagTest {
    public interface TestHWOptions extends TestPipelineOptions, App.DagOptions {}

    private String dagType;

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
        // Test pipeline options as JSON array
        // https://beam.apache.org/releases/javadoc/2.57.0/org/apache/beam/sdk/testing/TestPipeline.html
        // https://www.json.org/json-en.html
        String[] pOpts = new String[]{
                "--runner=DirectRunner"
        };
        System.setProperty("beamTestPipelineOptions", asJsonStr(pOpts));
        dagType = "HELLOWORLD";
    }

    @Test
    @Category(NeedsRunner.class)
    public void testProcess() {
        p
                .getOptions()
                .as(App.DagOptions.class)
                .setDagType(dagType);
        p.run();
        assertTrue(true);
    }
}
