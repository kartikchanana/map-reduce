import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class pagerank {

    //Map class takes input and emits key as nodeName, value string as (pagerank, list of neighbours)
    public static class Map extends Mapper<Text, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue  = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if(key.toString() != null || !key.toString().trim().isEmpty() || key.toString().isEmpty()){
                context.write(key, value);
            }
            Node node = new Node().createNodeObject(value.toString());
            //if the node has neighbours, give it's contribution to all neighbours
            if(node.getNeighbours() != null && node.getNeighbours().length > 0) {
                double outboundPageRank = node.getPageRank() /(double)node.getNeighbours().length;
                for (int i = 0; i < node.getNeighbours().length; i++) {
                    String neighbor = node.getNeighbours()[i];
                    //if any neighbour is an empty string
                    if(!neighbor.trim().isEmpty() || !neighbor.isEmpty() || neighbor != null) {
                        outKey.set(neighbor);
                        Node adjacentNode = new Node().setPageRank(outboundPageRank);
                        outValue.set(adjacentNode.toString());
                        context.write(outKey, outValue);
                    }
                }
            }else {
                //increment dangling node counter
                context.getCounter(nodesCounter.danglingCounter).increment(1L);
            }
        }
    }

    //Reduce class gets node name as key string, (pageRank, list of nodes), outputs node name
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public final double DAMPING_FACTOR = 0.85;
        private Long numberOfNodesInGraph;


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
             numberOfNodesInGraph = Long.parseLong(context.getConfiguration().get("totalPages"));
        }

        private Text outValue = new Text();

        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

            double summedPageRanks = 0;
            Node originalNode = new Node();

            Long danglingCount = context.getCounter(nodesCounter.danglingCounter).getValue();
            Long alpha = danglingCount/numberOfNodesInGraph;
            //iterate over all values and calculate summed page rank adding all contributions
            for (Text textValue : values) {
                Node node = new Node().createNodeObject(textValue.toString());
                if (node.hasNeighbours()) {
                    originalNode = node;
                } else {
                    summedPageRanks += node.getPageRank();
                    summedPageRanks += alpha;
                }
            }
            double dampingFactor = ((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);

            double newPageRank = dampingFactor + (DAMPING_FACTOR * summedPageRanks);
            originalNode.setPageRank(newPageRank);
            outValue.set(originalNode.toString());
            context.write(key, outValue);
        }
    }

}
