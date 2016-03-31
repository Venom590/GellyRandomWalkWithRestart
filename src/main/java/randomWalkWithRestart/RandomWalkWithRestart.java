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
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import randomWalkWithRestart.Functions.RandomWalkWithRestartMessagingFunction;
import randomWalkWithRestart.Functions.RandomWalkWithRestartVertexUpdateFunction;

/**
 * Class implements the random-walk-with-restart algorithm. When run on a graph
 * it returns a similar graph in which the vertex-values are the vertices' state
 * probabilities.
 */
public class RandomWalkWithRestart implements GraphAlgorithm<Long, Double, Double, Graph<Long, Double, Double>> {

    public static final String MAX_ITERATIONS = RandomWalkWithRestart.class.getName() + ".maxSupersteps";
    public static final String TELEPORTATION_PROBABILITY = RandomWalkWithRestart.class.getName() + ".teleportationProbability";
    public static final String SOURCE_VERTEX = RandomWalkWithRestart.class.getName() + ".sourceVertex";
    public static final String CUMULATIVE_PROBABILITY = RandomWalkWithRestart.class.getName() + ".cumulativeProbability";
    public static final String CUMULATIVE_DANGLING_PROBABILITY = RandomWalkWithRestart.class.getName() + ".cumulativeDanglingProbability";
    public static final String NUM_DANGLING_VERTICES = RandomWalkWithRestart.class.getName() + ".numDanglingVertices";
    public static final String L1_NORM_OF_PROBABILITY_DIFFERENCE = RandomWalkWithRestart.class.getName() + ".l1NormOfProbabilityDifference";
    public static final String NUM_SOURCE_VERTICES = RandomWalkWithRestart.class.getName() + ".numSourceVertices";

    private Configuration conf;

    /**
     * Constructor, sets configurations
     *
     * @param sourceVertices List of source vertices' IDs
     * @param teleportationProbability Teleportation probability
     * @param maxIterations Maximum number of iterations to be executed
     */
    public RandomWalkWithRestart(List<Long> sourceVertices, Double teleportationProbability, int maxIterations) {
        conf = new Configuration();
        conf.setInteger(RandomWalkWithRestart.MAX_ITERATIONS, maxIterations);
        conf.setDouble(RandomWalkWithRestart.TELEPORTATION_PROBABILITY, teleportationProbability);
        conf.setInteger(RandomWalkWithRestart.NUM_SOURCE_VERTICES, sourceVertices.size());
        for (int i = 0; i < sourceVertices.size(); i++) {
            conf.setLong(RandomWalkWithRestart.SOURCE_VERTEX + "_" + i, sourceVertices.get(i));
        }
    }

    /**
     * Constructor for using only one source vertex (thus giving a long instead of List<long>)
     * 
     * @param sourceVertex ID of source vertex
     * @param teleportationProbability Teleportation probability
     * @param maxIterations Maximum number of iterations to be executed
     */    
    public RandomWalkWithRestart(Long sourceVertex, Double teleportationProbability, int maxIterations) {
        this(new ArrayList<>(Arrays.asList(new Long[]{sourceVertex})), teleportationProbability, maxIterations);
    }

    /**
     * Runs the algorithm on a given graph
     * 
     * @param graph Graph on which the algorithm runs
     * @return Graph containing the vertices' state probabilities as values
     * @override
     * @throws Exception
     */
    @Override
    public Graph<Long, Double, Double> run(Graph<Long, Double, Double> graph) throws Exception {
        VertexCentricConfiguration parameters = new VertexCentricConfiguration();
        parameters.setName("Gelly Iteration");
        parameters.setOptDegrees(true);
        parameters.setOptNumVertices(true);

        parameters.registerAggregator(RandomWalkWithRestart.L1_NORM_OF_PROBABILITY_DIFFERENCE, new DoubleSumAggregator());
        parameters.registerAggregator(RandomWalkWithRestart.CUMULATIVE_PROBABILITY, new DoubleSumAggregator());
        parameters.registerAggregator(RandomWalkWithRestart.NUM_DANGLING_VERTICES, new DoubleSumAggregator());
        parameters.registerAggregator(RandomWalkWithRestart.CUMULATIVE_DANGLING_PROBABILITY, new DoubleSumAggregator());

        return graph.runVertexCentricIteration(new RandomWalkWithRestartVertexUpdateFunction(conf), new RandomWalkWithRestartMessagingFunction(conf),
                conf.getInteger(RandomWalkWithRestart.MAX_ITERATIONS, 0), parameters);
    }
}
