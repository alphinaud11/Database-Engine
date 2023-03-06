package MOG_SQL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class Leaf extends Node {

    String nextLeafPath;
    String prevLeafPath;
    ArrayList<TuplePointer> pointers;

    public Leaf(Object tree, String parent) {
        super(tree, parent);
        if (tree instanceof BPTree)
            this.min = (((BPTree) tree).n+1)/2;
        else
            this.min = (((RTree) tree).n+1)/2;
        pointers = new ArrayList<TuplePointer>();
    }

    public void insertSorted(TuplePointer tp, String pagePath, Overflow newOverflow) throws IOException, ClassNotFoundException  //modified by Helmy
    {
        int i = pointers.size()-1;
        boolean duplicate = false;


        if (DBApp.compare(pointers.get(i).key,tp.key) == 0)
            duplicate = true;

        while(i>=0 && DBApp.compare(pointers.get(i).key,tp.key) > 0)
        {
            i--;
            if (i >= 0) {
                if (DBApp.compare(pointers.get(i).key,tp.key) == 0)
                    duplicate = true;
            }
        }

        if (!duplicate) {
            pointers.add(i+1, tp);
            DBApp.writeObject(newOverflow,tp.overflowPath);
        }
        else {
            Overflow overflowPage = (Overflow) DBApp.readObject(pointers.get(i).overflowPath);
            overflowPage.addPage(pagePath);
            DBApp.writeObject(overflowPage,pointers.get(i).overflowPath);
            overflowPage = null;
        }
    }

    public ArrayList<TuplePointer> getSecondHalf(){
        int half = (int) Math.floor((max+1)/2.0);
        ArrayList<TuplePointer> secondHalf = new ArrayList<TuplePointer>();
        while(half<pointers.size()){
            secondHalf.add(pointers.remove(half));
        }
        return secondHalf;

    }

    public int deleteKey(Object key, String page) throws IOException, ClassNotFoundException {  //modified by Helmy
        for(int i=0; i<pointers.size(); i++) {
            if(DBApp.compare(pointers.get(i).key,key) == 0){
                Overflow overflowPage = (Overflow) DBApp.readObject(pointers.get(i).overflowPath);
                overflowPage.delete(page);
                if (overflowPage.pagesAndOccurrences.size() == 0) {
                    File file = new File(pointers.get(i).overflowPath);
                    file.delete();
                    TuplePointer deleted = pointers.remove(i);
                    overflowPage = null;
                    return i;
                }
                else {
                    DBApp.writeObject(overflowPage,pointers.get(i).overflowPath);
                    overflowPage = null;
                    return -1;
                }
            }
        }
        return -1;
    }

    public void borrowTuple(Object tree, Leaf sibling, NonLeaf parent, boolean left,int parentIdx,Object dKey) throws ClassNotFoundException, IOException
    {
        boolean willUpdateUpper = this.pointers.size() == 0 || DBApp.compare(this.pointers.get(0).key,dKey) > 0;
        if(left)
        {
//			willUpdateUpper = false;
            TuplePointer toBeBorrwed = sibling.pointers.remove(sibling.pointers.size()-1);
            this.pointers.add(0,toBeBorrwed);
            //update parent
            parent.entries.get(parentIdx-1).key = toBeBorrwed.key;
        }
        else
        {
//			willUpdateUpper &= parentIdx == 0;
            TuplePointer toBeBorrwed = sibling.pointers.remove(0);
            this.pointers.add(toBeBorrwed);
            Object newParent = sibling.pointers.get(0).key;
            parent.entries.get(parentIdx).key = newParent;

//			if(willUpdateUpper)
            {

                if(parentIdx==0){
                    if (tree instanceof BPTree)
                        ((BPTree) tree).updateUpper(dKey, toBeBorrwed.key, this.parent);
                    else
                        ((RTree) tree).updateUpper(dKey, toBeBorrwed.key, this.parent);
                }
                else if(willUpdateUpper)
                    parent.entries.get(parentIdx-1).key = this.pointers.get(0).key;
            }
        }

    }


    public void mergeWithLeaf(Object tree, Leaf sibling, NonLeaf parent, int parentIdx, boolean left, Object dKey) throws ClassNotFoundException, IOException
    {
        String tmpPath = null;
        boolean willUpdateUpper = this.pointers.size() == 0 || DBApp.compare(this.pointers.get(0).key,dKey) > 0;
        Object newKey = null;
        if(left)
        {
            willUpdateUpper = false;
            this.pointers.addAll(0,sibling.pointers);
            if(parentIdx > 0)
            {
                if(parentIdx > 1){
                    parent.entries.get(parentIdx - 2).right = parent.entries.get(parentIdx-1).right;
                }
                else if(parent.entries.size() == 1)
                {
                    tmpPath = parent.entries.get(parentIdx-1).right;
                }
                parent.entries.remove(parentIdx - 1);

            }
        }
        else
        {
            willUpdateUpper &= parentIdx == 0;
            sibling.pointers.addAll(0,this.pointers);

            if(parentIdx > 0){
                parent.entries.get(parentIdx - 1).right = parent.entries.get(parentIdx).right;
            }
            else if(parent.entries.size() == 1)
            {
                tmpPath = parent.entries.get(parentIdx).right;
            }
            parent.entries.remove(parentIdx);
            newKey = sibling.pointers.get(0).key;
        }
        if(willUpdateUpper){
            if (tree instanceof BPTree)
                ((BPTree) tree).updateUpper(dKey, newKey, this.parent);
            else
                ((RTree) tree).updateUpper(dKey, newKey, this.parent);
        }
        if (tree instanceof BPTree)
            ((BPTree) tree).handleParent(parent, this.parent,tmpPath);
        else
            ((RTree) tree).handleParent(parent, this.parent,tmpPath);
    }


    public String toString(){
        String res = "Start LEAF\n";

        for(TuplePointer tp : this.pointers){
            res += tp.key+" ";
        }
        return res+"\nEnd LEAF\n";
    }



}
