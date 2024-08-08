package org.example.dags;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.example.App;

public interface Dag {
    void process(Pipeline p);
}
