package org.example;

import org.example.dags.Dag;
import org.example.dags.helloworld.HelloWorldDag;
import org.example.dags.realestate.RealEstateDag;
import org.junit.Before;
import org.junit.Test;

import static org.example.DagDispatcher.dispatch;
import static org.junit.Assert.assertTrue;

public class DagTypeTest {
    private final String DAGTYPE_HELLOWORLD = "HELLOWORLD";
    private final String DAGTYPE_REALESTATE = "REALESTATE";

    @Before
    public void setUp() {}

    @Test
    public void testDispatchHelloWorld() {
        assertTrue(dispatch(DagType.valueOf(DAGTYPE_HELLOWORLD)) instanceof HelloWorldDag);
    }
    @Test
    public void testDispatchRealEstate() {
        assertTrue(dispatch(DagType.valueOf(DAGTYPE_REALESTATE)) instanceof RealEstateDag);
    }

}
