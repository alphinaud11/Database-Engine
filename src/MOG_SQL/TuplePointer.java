package MOG_SQL;

import java.io.Serializable;

public class TuplePointer implements Comparable<TuplePointer>,Serializable{
    private static final long serialVersionUID = 6529685098267757690L;
    Object key;
    String overflowPath;



    public TuplePointer(Object key, String overflowPath){
        this.key = key;
        this.overflowPath = overflowPath;
    }

    @Override
    public int compareTo(TuplePointer tp) {
        return DBApp.compare(this.key,tp.key);
    }

    public Object getKey() {
        return key;
    }

    public String getOverflow(){
        return this.overflowPath;
    }
}
