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

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.junit.Test;

/**
 * Test class for @link {RandomWalkWithRestart}
 */
public class RandomWalkWithRestartTest {

    /**
     * Test method for {@link randomWalkWithRestart.RandomWalkWithRestart#run(org.apache.flink.graph.Graph)}.
     * Values where given by the RandomWalkWithRestart implementation of giraph
     */
    @Test
    public void testRun() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();	

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


        Graph<Long, Double, Double> graph = Graph.fromCollection(vertices, edges, env);
        Graph<Long, Double, Double> graph2 = null;
        ArrayList<Long> sourceVertices = new ArrayList<Long>();
        sourceVertices.add(12L);
        try {
            graph2 = graph.run(new RandomWalkWithRestart(sourceVertices, 0.15d, 30));
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<Vertex<Long, Double>> result = null;
        try {
            result = graph2.getVertices().collect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Vertex<Long, Double> vertex : result) {
            // 0.163365, 0.378932, 0.156886, 0.300816 calculated externally
            if (vertex.getId().equals(v1.getId())) {
                    assertEquals(Double.valueOf(0.163365d), vertex.getValue(), 0.002d);
            }
            if (vertex.getId().equals(v2.getId())) {
                    assertEquals(Double.valueOf(0.378932d), vertex.getValue(), 0.002d);
            }
            if (vertex.getId().equals(v3.getId())) {
                    assertEquals(Double.valueOf(0.156886d), vertex.getValue(), 0.002d);
            }
            if (vertex.getId().equals(v4.getId())) {
                    assertEquals(Double.valueOf(0.300816d), vertex.getValue(), 0.002d);
            }
        }
    }
}
