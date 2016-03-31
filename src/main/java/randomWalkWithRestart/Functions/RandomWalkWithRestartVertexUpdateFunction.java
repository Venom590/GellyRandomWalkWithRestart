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

import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.DoubleValue;
import randomWalkWithRestart.RandomWalkWithRestart;
import randomWalkWithRestart.Utils.MathUtils;

/**
 * Implements the vertex update function for the RandomWalkWithRestart class
 */
public class RandomWalkWithRestartVertexUpdateFunction extends VertexUpdateFunction<Long, Double, MessageType<Double>>{
    private static final long serialVersionUID = 1L;
    private Configuration conf;
    private DoubleSumAggregator aggregator;

    /**
     * Constructor
     * @param conf Configuration to use 
     */
    public RandomWalkWithRestartVertexUpdateFunction(Configuration conf) {
            this.conf = conf;
    }

    /**
     * Updates the vertex's value to its new state probability
     * 
     * @param vertex Vertex that is to be updated
     * @param inMessages All incoming messages from the previous superstep (containing the absolute transition probabilities to end up in the given vertex)
     * @override
     * @throws Exception 
     */
    @Override
    public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<MessageType<Double>> inMessages) throws Exception {		
        BigDecimal stateProbability;
        BigDecimal difference;
        //giraph starts with 0 gelly with 1
        if (this.getSuperstepNumber() > 1) {
            BigDecimal previousStateProbability = BigDecimal.valueOf(vertex.getValue());

            stateProbability = this.calcNewStateProbability(vertex, inMessages, teleportationProbability()); 

            stateProbability = stateProbability.divide(this.getPreviousCumulativeProbability(), MathContext.DECIMAL128);
            difference = stateProbability.subtract(previousStateProbability);

            this.aggregator = this.getIterationAggregator(RandomWalkWithRestart.L1_NORM_OF_PROBABILITY_DIFFERENCE);
            this.aggregator.aggregate(difference.doubleValue());
        } else {
                stateProbability = this.initialProbability();
        }
        
        this.setNewVertexValue(stateProbability.doubleValue());
        this.aggregator = this.getIterationAggregator(RandomWalkWithRestart.CUMULATIVE_PROBABILITY);
        this.aggregator.aggregate(vertex.getValue());


        if (this.getOutDegree() == 0) { 
            this.aggregator = this.getIterationAggregator(RandomWalkWithRestart.NUM_DANGLING_VERTICES);
            this.aggregator.aggregate(1.0d);

            this.aggregator = this.getIterationAggregator(RandomWalkWithRestart.CUMULATIVE_DANGLING_PROBABILITY);
            this.aggregator.aggregate(vertex.getValue());
        }
    }

    /**
     * Calculates the new state probability for a vertex based upon the previous superstep
     * 
     * @param vertex Vertex whose state probability is to be calculated
     * @param inMessages All incoming messages from the last superstep for this vertex
     * @param teleportationProbability Teleportaion probability
     * @return Updated state probability
     */        
    protected BigDecimal calcNewStateProbability(Vertex<Long, Double> vertex, MessageIterator<MessageType<Double>> inMessages, BigDecimal teleportationProbability ) {
        int numSourceVertices = conf.getInteger(RandomWalkWithRestart.NUM_SOURCE_VERTICES, 0);
        if (numSourceVertices <= 0) {
            System.err.println("ERROR: No source vertex found");
        }
        BigDecimal stateProbability = MathUtils.sum(inMessages);

        stateProbability = stateProbability.add(this.getDanglingProbability().divide(
            BigDecimal.valueOf(this.getNumberOfVertices())));

        stateProbability = stateProbability.multiply(BigDecimal.ONE.subtract(teleportationProbability));

        for (int i = 0; i < conf.getInteger(RandomWalkWithRestart.NUM_SOURCE_VERTICES, 0); i++) {
            if (vertex.getId() == conf.getLong(RandomWalkWithRestart.SOURCE_VERTEX + "_" + i, 0L)){
                    stateProbability = stateProbability.add(teleportationProbability.divide(BigDecimal.valueOf(numSourceVertices)));
            }
        }
        return stateProbability;
    }

    /**
     * Returns the initial probability which is the same for all vertices
     * 
     * @return Initial probability, i.e. 1/number-of-vertices
     */	
    protected BigDecimal initialProbability() {
        return BigDecimal.ONE.divide(BigDecimal.valueOf(this.getNumberOfVertices()));
    }
	
    /**
     * Due to rounding errors the probabilities do not sum up to exactly 1.
     * Therefore the cumulativeProbability is needed to minimize these errors.
     * 
     * @return Cumulative probability of the previous superstep
     */        
    protected BigDecimal getPreviousCumulativeProbability() {
        DoubleValue prevComProb = this.getPreviousIterationAggregate(RandomWalkWithRestart.CUMULATIVE_PROBABILITY);
        return BigDecimal.valueOf(prevComProb.getValue());
    }

    /**
     * Returns the dangling proabability, i.e. the probability to be in a vertex that has no outgoing edges
     * 
     * @return Returns the probability to be in a dangling vertex in the last superstep
     */        
    protected BigDecimal getDanglingProbability() {
        return BigDecimal.valueOf(((DoubleSumAggregator) this.getIterationAggregator(RandomWalkWithRestart.CUMULATIVE_DANGLING_PROBABILITY)).getAggregate().getValue());
    }

    /**
     * Returns the (constant) teleportation probability
     * @return Teleportation Probability
     */
    protected BigDecimal teleportationProbability() {
        return  BigDecimal.valueOf(conf.getDouble(RandomWalkWithRestart.TELEPORTATION_PROBABILITY, 0.0f));
    }
}
