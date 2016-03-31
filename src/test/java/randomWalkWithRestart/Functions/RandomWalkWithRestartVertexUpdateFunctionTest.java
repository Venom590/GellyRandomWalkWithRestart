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
import java.math.BigDecimal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.junit.Test;
import randomWalkWithRestart.RandomWalkWithRestart;

/**
 * Test class for {@RandomWalkWithRestartVertexUpdateFunction}
 */
public class RandomWalkWithRestartVertexUpdateFunctionTest {
    /**
     * Test method for {@link randomWalkWithRestart.Functions.RandomWalkWithRestartVertexUpdateFunction#updateVertex(org.apache.flink.graph.Vertex, org.apache.flink.graph.spargel.MessageIterator)}.
     */
    @Test
    public void testCalcNewStateProbability() {
        Configuration conf = new Configuration();
        conf.setInteger(RandomWalkWithRestart.NUM_SOURCE_VERTICES, 1);
        conf.setLong(RandomWalkWithRestart.SOURCE_VERTEX, 1L );

        RandomWalkWithRestartVertexUpdateFunctionForTests uF = new RandomWalkWithRestartVertexUpdateFunctionForTests(conf);
        BigDecimal newStateProbability;
        Vertex<Long, Double> vertex;
        MessageIterator<MessageType<Double>> inMessages; 
        BigDecimal teleportationProbability;

        vertex = new Vertex<>();

        inMessages = new MessageIterator<>();
        teleportationProbability = BigDecimal.valueOf(0.15d);

        newStateProbability = uF.calcNewStateProbability(vertex, inMessages, teleportationProbability);

        assertEquals(BigDecimal.ZERO, newStateProbability);
    }

    /**
     * Test method for {@link randomWalkWithRestart.Functions.RandomWalkWithRestartVertexUpdateFunction#initialProbability()}.
     */
    @Test
    public void testInitialProbability() {
            RandomWalkWithRestartVertexUpdateFunctionForTests uF = new RandomWalkWithRestartVertexUpdateFunctionForTests(new Configuration());
            BigDecimal iP = uF.initialProbability();
            assertEquals(BigDecimal.valueOf(0.2d), iP);
    }


    private class RandomWalkWithRestartVertexUpdateFunctionForTests extends RandomWalkWithRestartVertexUpdateFunction {

        private static final long serialVersionUID = 1L;

        public RandomWalkWithRestartVertexUpdateFunctionForTests(Configuration conf) {
                super(conf);
        }

        @Override
        public BigDecimal getDanglingProbability() {
             return BigDecimal.valueOf(1.0d);
        }

        @Override
        public long getNumberOfVertices() {
                return 5L;
        }
    }
}
