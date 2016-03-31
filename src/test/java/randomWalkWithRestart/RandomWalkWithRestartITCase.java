/*
  Copyright 2016 Jan Buchholz, Stephan Kemper

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package randomWalkWithRestart;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
/**
 * Test class, runs an example given by the RandomWalkWithRestart implementation of giraph
 */
@RunWith(Parameterized.class)
public class RandomWalkWithRestartITCase extends MultipleProgramsTestBase {
    
    public RandomWalkWithRestartITCase(TestExecutionMode arg0) {
        super(arg0);
    }

    @Test
    public void testPageRankWithThreeIterationsAndNumOfVertices() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String expected = "34,0.3771280405183819\n" +
                        "12,0.1633450894536872\n" +
                        "78,0.30243958799201975\n" +
                        "56,0.15708728203591124\n";

        Vertex<Long, Double> v1 = new Vertex<Long, Double>(12L, (0.25d));	
        Vertex<Long, Double> v2 = new Vertex<Long, Double>(34L, (0.25d));	
        Vertex<Long, Double> v3 = new Vertex<Long, Double>(56L, (0.25d));
        Vertex<Long, Double> v4 = new Vertex<Long, Double>(78L, (0.25d));	

        Edge<Long, Double> e1 = new Edge<Long, Double>(12L, 34L, 0.1d);
        Edge<Long, Double> e2 = new Edge<Long, Double>(12L, 56L, 0.9d);
        Edge<Long, Double> e3 = new Edge<Long, Double>(34L, 78L, 0.9d);
        Edge<Long, Double> e4 = new Edge<Long, Double>(34L, 56L, 0.1d);
        Edge<Long, Double> e5 = new Edge<Long, Double>(56L, 12L, 0.1d);
        Edge<Long, Double> e6 = new Edge<Long, Double>(56L, 34L, 0.8d);
        Edge<Long, Double> e7 = new Edge<Long, Double>(56L, 78L, 0.1d);
        Edge<Long, Double> e8 = new Edge<Long, Double>(78L, 34L, 1.0d);

        List<Vertex<Long, Double>> vertices;
        List<Edge<Long, Double>> edges;

        vertices = new ArrayList();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);
        vertices.add(v4);

        edges = new ArrayList();
        edges.add(e1);
        edges.add(e2);
        edges.add(e3);
        edges.add(e4);
        edges.add(e5);
        edges.add(e6);
        edges.add(e7);
        edges.add(e8);

        Graph<Long, Double, Double> inputGraph = Graph.fromCollection(vertices, edges, env);

        List<Vertex<Long, Double>> result = inputGraph.run(new RandomWalkWithRestart(12L, 0.15d, 30))
                        .getVertices().collect();
        System.err.println("result: " + result);

        compareResultAsTuples(result, expected);
    }
}
