import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Arrays;

//node object that stores a page rank and a list of neighbours
public class Node {

    private double pageRank;
    private String[] Neighbours;

    public double getPageRank() {
        return pageRank;
    }

    public Node setPageRank(double pageRank) {
        this.pageRank = pageRank;
        return this;
    }

    public String[] getNeighbours() {
        return Neighbours;
    }

    public Node setNeighbours(String[] neighbours) {
        this.Neighbours = neighbours;
        return this;
    }

    public boolean hasNeighbours() {
        return Neighbours != null;
    }

    //builds a string from the node data
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pageRank);

        if (getNeighbours() != null) {
            sb.append("\t").append(StringUtils.join(getNeighbours(), "\t"));
        }
        return sb.toString();
    }

    //creates a node object from input string
    public Node createNodeObject(String value) throws IOException {
        String[] parts = StringUtils.splitPreserveAllTokens(value, "\t");
        if (parts.length < 1) {
            throw new IOException("Expected atleast 2 objects " + parts.length);
        }
        Node node = new Node().setPageRank(Double.valueOf(parts[0]));
        if (parts.length > 1) {
            node.setNeighbours(Arrays.copyOfRange(parts, 1,parts.length));
        }
        return node;
    }
}

