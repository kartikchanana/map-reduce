import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class initialPagerank {

    static class Map extends Mapper<Text, Text, Text, Text> {

        private Long numberOfNodesInGraph;

        //get total pages count from context
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            numberOfNodesInGraph = Long.parseLong(context.getConfiguration().get("totalPages"));
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            double initialPageRank = 1.0/numberOfNodesInGraph;
            Node node = new Node();
            node.setPageRank(initialPageRank);

            String ls =value.toString();
            //if it has neighbours, set them in the node object
            if(ls.length() >1) {
                String listString = ls.substring(1, ls.length() - 1);
                String[] neighbours = listString.split(", ");
                node.setNeighbours(Arrays.copyOfRange(neighbours, 1, neighbours.length));
            }
            //emit the node
            context.write(key, new Text(node.toString()));
        }
    }
}
