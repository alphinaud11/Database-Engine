package MOG_SQL;

import java.io.Serializable;

public class Node implements Serializable{
    private static final long serialVersionUID = 6529685098267757690L;
    Object tree;
    int max;
    int min;
    String parent;

    public Node(Object tree, String parent)
    {
        this.parent = parent;
        this.tree = tree;
        if (tree instanceof BPTree)
            this.max = ((BPTree) tree).n;
        else
            this.max = ((RTree) tree).n;
    }
}
