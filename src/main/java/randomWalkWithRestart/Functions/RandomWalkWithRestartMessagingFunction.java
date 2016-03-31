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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import randomWalkWithRestart.RandomWalkWithRestart;
import java.math.BigDecimal;

/**
 * Implements the messaging function for the RandomWalkWithRestart class
 */
public class RandomWalkWithRestartMessagingFunction extends MessagingFunction<Long, Double, MessageType<Double>, Double> {
    private static final long serialVersionUID = 1L;
    private Configuration conf;
    
    /**
     * Constructor
     * @param conf Configuration to use
     */
    public RandomWalkWithRestartMessagingFunction(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Sends current transition probability to all outgoing edges of a vertex
     * @param vertex Vertex whose messages are sent
     * @throws Exception 
     * @override
     */
    @Override
    public void sendMessages(Vertex<Long, Double> vertex) throws Exception {

        double stateProbability;
        stateProbability = vertex.getValue();

        if (this.getSuperstepNumber() < conf.getInteger(RandomWalkWithRestart.MAX_ITERATIONS, 0)) {
            for (Edge<Long, Double> edge : this.getEdges()) {
                double transitionProbability = this.transitionProbability(vertex, stateProbability, edge);
                this.sendMessageTo(edge.getTarget(), new MessageType<>(transitionProbability));
            }
        }
    }

    /**
     * Returns the transition probability for a vertex to lead to another specific neighbor
     * 
     * @param vertex Current vertex
     * @param stateProbability The vertex's current state probability
     * @param edge Edge leading from the current vertex to one of its neighbors
     * @return Probability to follow the given edge from the source vertex to one of its neighbors
     */
    protected double transitionProbability(Vertex<Long, Double> vertex, double stateProbability, Edge<Long, Double> edge) {
        BigDecimal sP = BigDecimal.valueOf(stateProbability);
        BigDecimal eV = BigDecimal.valueOf(edge.getValue());

        BigDecimal times = sP.multiply(eV);

        return times.doubleValue();
    }
}
