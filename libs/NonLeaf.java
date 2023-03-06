import java.io.IOException;
import java.util.ArrayList;

public class NonLeaf extends Node {

    ArrayList<NodeEntry> entries;

    public NonLeaf(Object tree, String parent) {
        super(tree, parent);
        if (tree instanceof BPTree)
            this.min = (int) Math.ceil((((BPTree) tree).n+1)/2.0) - 1;
        else
            this.min = (int) Math.ceil((((RTree) tree).n+1)/2.0) - 1;
        entries = new ArrayList<NodeEntry>();

    }

    public void insertSorted(NodeEntry e)
    {
        int i = entries.size()-1;
        while(i>=0 && DBApp.compare(entries.get(i).key,e.key) > 0)
        {
            i--;
        }
        entries.add(i+1, e);
        if(i>= 0 )
            entries.get(i).right = e.left;
        if(i+2 < entries.size())
            entries.get(i+2).left = e.right;


    }

    public ArrayList<NodeEntry> getSecondHalf(){
        int half = (int) Math.floor((max+1)/2.0);
        ArrayList<NodeEntry> secondHalf = new ArrayList<NodeEntry>();
        while(half<entries.size()){
            secondHalf.add(entries.remove(half));
        }
        return secondHalf;

    }


    public void mergeWithNonLeaf(Object tree, NonLeaf sibling, NonLeaf parent, boolean left, int parentIdx,String inTmpPath) throws ClassNotFoundException, IOException{


        String outTmpPath = null;
        NodeEntry p = null;

        if(left)
        {

            if(parentIdx > 0){

                p = parent.entries.remove(parentIdx-1);

//				if(parent.entries.size() == 0)
                outTmpPath = p.right;
//
//				else{
                if(parentIdx > 0 && parentIdx <= parent.entries.size() )
                    parent.entries.get(parentIdx-1).left = p.right;
                if(parentIdx > 1 && parentIdx <= parent.entries.size() + 1)
                    parent.entries.get(parentIdx-2).right = p.right;
//				}
                if(this.entries.size() == 0)
                    p.right = inTmpPath;
                else
                    p.right = this.entries.get(0).left;
                this.entries.add(0,p);
                p.left = sibling.entries.get(sibling.entries.size()-1).right;
                this.entries.addAll(0,sibling.entries);

            }
        }
        else
        {

            p = parent.entries.remove(parentIdx);
//			if(parent.entries.size() == 0)
            outTmpPath = p.right;
//			else{
            if(parentIdx >= 0 && parentIdx < parent.entries.size())
                parent.entries.get(parentIdx).left = p.right;
            if(parentIdx >= 1 && parentIdx  <= parent.entries.size())
                parent.entries.get(parentIdx-1).right = p.right;
//			}
            if(this.entries.size() == 0)
                p.left = inTmpPath;
            else
                p.left = this.entries.get(this.entries.size()-1).right;

            p.right = sibling.entries.get(0).left;
            sibling.entries.add(0,p);

            sibling.entries.addAll(0,this.entries);
        }

        DBApp.writeObject(this, outTmpPath);

        if (tree instanceof BPTree)
            ((BPTree) tree).handleParent(parent, this.parent, outTmpPath);
        else
            ((RTree) tree).handleParent(parent, this.parent, outTmpPath);

    }


    public void borrow(NonLeaf sibling, NonLeaf parent, boolean left, int parentIdx, String tmpPath)
    {

        if(left)
        {
            NodeEntry siblingBorrowed = sibling.entries.remove(sibling.entries.size()-1);
            Object parentBorrowed = parent.entries.get(parentIdx-1).key;
            parent.entries.get(parentIdx - 1).key = siblingBorrowed.key;
            if(this.entries.size() > 0)
                this.entries.add(0,new NodeEntry(parentBorrowed,siblingBorrowed.right,this.entries.get(0).left));
            else
            {
                this.entries.add(0,new NodeEntry(parentBorrowed,siblingBorrowed.right,tmpPath));
            }


        }
        else
        {
            NodeEntry siblingBorrowed = sibling.entries.remove(0);
            Object parentBorrowed = parent.entries.get(parentIdx).key;
            parent.entries.get(parentIdx).key = siblingBorrowed.key;
            if(entries.size() > 0)
                this.entries.add(new NodeEntry(parentBorrowed,this.entries.get(this.entries.size() - 1).right,siblingBorrowed.left));
            else
            {
                this.entries.add(new NodeEntry(parentBorrowed,tmpPath,siblingBorrowed.left));
            }
        }
    }




    public String toString(){
        String res = "Start NON LEAF\n";

        for(NodeEntry ent : this.entries){
            res += ent.key+" ";
        }
        return res+"\nEnd NON LEAF\n";
    }




}
