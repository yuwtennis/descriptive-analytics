package org.example;

import org.example.dags.Dag;
import org.example.dags.helloworld.HelloWorldDag;
import org.example.dags.realestate.RealEstateDag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DagDispatcher {
    static Logger LOG = LoggerFactory.getLogger(DagDispatcher.class);
    /***
     * Dispatches with the correct dag to the app client
     * @param dagType
     * @return
     */
    public static Dag dispatch(DagType dagType) {
        Dag d = null;

        switch (dagType) {
            case DagType.HELLOWORLD:
                d = new HelloWorldDag();
                break;
            case DagType.REALESTATE:
                d = new RealEstateDag();
                break;
            default:
        }

        LOG.info("Running dag type as {}", d);

        return d;
    }
}
