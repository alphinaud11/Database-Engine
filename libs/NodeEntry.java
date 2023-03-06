import java.io.Serializable;

public class NodeEntry implements Serializable{
    private static final long serialVersionUID = 6529685098267757690L;
    Object key;
    String left;
    String right;
    public NodeEntry(Object key, String left, String right)
    {
        this.key	= key;
        this.left	= left;
        this.right	= right;
    }
}
