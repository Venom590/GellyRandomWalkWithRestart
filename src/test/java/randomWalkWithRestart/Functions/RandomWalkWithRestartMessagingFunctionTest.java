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
package randomWalkWithRestart.Functions;

import static org.junit.Assert.assertEquals;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.Test;

/**
 * Test class for @link {RandomWalkWithRestartMessagingFunction}
 */
public class RandomWalkWithRestartMessagingFunctionTest {
    
    /**
     * Test method for {@link randomWalkWithRestart.Functions.RandomWalkWithRestartMessagingFunction#transitionProbability(org.apache.flink.graph.Vertex, double, org.apache.flink.graph.Edge)}.
     */
    @Test
    public void testTransitionProbability() {
        RandomWalkWithRestartMessagingFunction mf;
        mf = new RandomWalkWithRestartMessagingFunction(new Configuration());

        Vertex<Long, Double> vertex;
        double stateProbability;
        Edge<Long, Double> edge;

        vertex = new Vertex<>();
        stateProbability = 1.1d;
        edge = new Edge<>(1L, 2L, 0.1d);

        assertEquals(Double.valueOf(0.11d), Double.valueOf(mf.transitionProbability(vertex, stateProbability, edge)));
    }
}
