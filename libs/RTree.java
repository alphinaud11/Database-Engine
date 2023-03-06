import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class RTree implements Serializable{
    private static final long serialVersionUID = 6529685098267757690L;
    int n;
    Node root;
    String rootPath;
    String tableName;
    String column;
    int countNodes;
    String pathToTree;
    int minLeaf;
    int minNonLeaf;
    public RTree(int n, String tableName, String column) throws FileNotFoundException, IOException{
        this.n = n;
        this.tableName = tableName;
        this.column = column;
        countNodes = 1;
        root = new Leaf(this,null);
        root.min = 1;
        minLeaf = (n+1)/2;
        minNonLeaf = (int) Math.ceil((n+1)/2.0) - 1;
        pathToTree = "data/"+tableName+"/indices/"+column;
        rootPath = pathToTree+"/0.class";
        new File(pathToTree).mkdirs();
        DBApp.writeObject(this, pathToTree+"/Rtree.class");
        DBApp.writeObject(root, rootPath);
    }


    public void insert(Object key, String pagePath) throws ClassNotFoundException, IOException  //modified by Helmy
    {

        String pathToLeaf = findLeaf(rootPath, key,null);



        Leaf leaf = (Leaf) DBApp.readObject(pathToLeaf);
        if(pathToLeaf.equals(rootPath)){
            this.root = leaf;
        }
        String overflowPath = pathToTree + "/overflow" + key.toString() + ".class";
        TuplePointer newPointer = new TuplePointer(key,overflowPath);
        Overflow newOverflow = new Overflow();
        newOverflow.addPage(pagePath);
        if(leaf.pointers.size() == 0){

            leaf.pointers.add(newPointer);
            DBApp.writeObject(leaf, pathToLeaf);
            DBApp.writeObject(newOverflow, overflowPath);
            return;
        }
        leaf.insertSorted(newPointer, pagePath, newOverflow);

        if(leaf.pointers.size() > leaf.max)
        {



            Leaf newLeaf = new Leaf(this, leaf.parent);
            newLeaf.nextLeafPath = leaf.nextLeafPath;
            newLeaf.prevLeafPath = pathToLeaf;

            String newPath = pathToTree+"/"+countNodes++ + ".class";

            leaf.nextLeafPath = newPath;


            newLeaf.pointers = leaf.getSecondHalf();


            DBApp.writeObject(newLeaf, newPath);
            DBApp.writeObject(leaf, pathToLeaf);

            insertIntoNonLeaf(leaf.parent, new NodeEntry(newLeaf.pointers.get(0).key,pathToLeaf,newPath));

        }

        DBApp.writeObject(leaf, pathToLeaf);
        DBApp.writeObject(this, pathToTree+"/Rtree.class");

    }

    private void insertIntoNonLeaf(String pathToNode, NodeEntry ne) throws ClassNotFoundException, IOException
    {

        if(pathToNode == null)
        {

            NonLeaf root = new NonLeaf(this, null);

            {

                this.root.min = (int)Math.ceil((n+1)/2.0) - 1;


                DBApp.writeObject(this.root, this.rootPath);


            }

            root.min = 1;
            rootPath = pathToTree+"/"+countNodes++ + ".class";
            root.entries.add(ne);

            this.root = root;
            DBApp.writeObject(this, pathToTree+"/Rtree.class");
            DBApp.writeObject(root, rootPath);

            return;
        }

        NonLeaf nl = (NonLeaf) DBApp.readObject(pathToNode);

        nl.insertSorted(ne);



        if(nl.entries.size() > nl.max)
        {

            ArrayList<NodeEntry> nes = nl.getSecondHalf();

            NonLeaf newNode = new NonLeaf(this, nl.parent);
            String newPath = pathToTree+"/"+(countNodes++) + ".class";
            newNode.entries = nes;

            NodeEntry first = nes.remove(0);

            first.left = pathToNode;
            first.right = newPath;


            DBApp.writeObject(newNode, newPath);
            insertIntoNonLeaf(nl.parent,first);


        }
        DBApp.writeObject(nl, pathToNode);

    }

    public void delete(Object key, String page) throws ClassNotFoundException, IOException
    {
        String pathToLeaf = findLeaf(rootPath, key, null);
        Leaf leaf = (Leaf) DBApp.readObject(pathToLeaf);
        int deletedIdx = leaf.deleteKey(key, page);


        if(leaf.parent == null){
            DBApp.writeObject(leaf, pathToLeaf);
            return;
        }
        if(leaf.pointers.size() < leaf.min){
            NonLeaf parent = (NonLeaf) DBApp.readObject(leaf.parent);

            LeftAndRightSiblings lrs = getSibLings(pathToLeaf, parent);

            String siblingLeft = lrs.sibLingLeft;
            String siblingRight = lrs.sibLingRight;
            int parentIdx = lrs.idx;

            Leaf leftLeaf = null;
            Leaf rightLeaf = null;
            if(siblingLeft != null)
            {

                // borrow from left

                leftLeaf = (Leaf) DBApp.readObject(siblingLeft);

                if(leftLeaf.pointers.size() > leftLeaf.min)
                {

                    leaf.borrowTuple(this, leftLeaf, parent, true, parentIdx, key);
                    DBApp.writeObject(leftLeaf, siblingLeft);
                }
                else if(siblingRight != null)
                {


                    rightLeaf = (Leaf) DBApp.readObject(siblingRight);
                    if(rightLeaf.pointers.size() > rightLeaf.min)
                    {
                        leaf.borrowTuple(this, rightLeaf, parent, false, parentIdx, key);
                        DBApp.writeObject(rightLeaf, siblingRight);
                    }
                    else
                    {
                        leaf.mergeWithLeaf(this, leftLeaf, parent, parentIdx, true, key);
                        DBApp.writeObject(leftLeaf, siblingLeft);
                    }
                }
                else
                {
                    leaf.mergeWithLeaf(this, leftLeaf, parent, parentIdx, true, key);
                    DBApp.writeObject(leftLeaf, siblingLeft);
                }

            }
            else if(siblingRight != null)
            {

                // borrow from right
                rightLeaf = (Leaf) DBApp.readObject(siblingRight);
                if(rightLeaf.pointers.size() > rightLeaf.min)
                {
                    leaf.borrowTuple(this, rightLeaf, parent, false, parentIdx, key);
                    DBApp.writeObject(rightLeaf, siblingRight);
                }
                else
                {
                    leaf.mergeWithLeaf(this, rightLeaf, parent, parentIdx, false, key);
                    DBApp.writeObject(rightLeaf, siblingRight);
                }
            }
            else
            {


            }

            DBApp.writeObject(leaf, pathToLeaf);
            DBApp.writeObject(parent, leaf.parent);





        }
        else
        {
            if(deletedIdx == 0)
            {
                Object newKey = leaf.pointers.get(0).key;
                updateUpper(key,newKey,leaf.parent);
            }
            DBApp.writeObject(leaf, pathToLeaf);

        }

        DBApp.writeObject(this, pathToTree+"/Rtree.class");

    }



    protected void handleParent(NonLeaf currentNode, String pathToNode, String tmpPath) throws ClassNotFoundException, IOException
    {
        if(pathToNode.equals(rootPath))
            this.root = currentNode;

        if(currentNode.entries.size() >= currentNode.min)
            return;
        if(currentNode.parent == null)
        {

            // current node is the root
            if(currentNode.entries.size() == 0){


                this.root.min = (int) Math.ceil((this.n+1)/2.0) - 1;
                DBApp.writeObject(this.root, this.rootPath);

                this.rootPath = tmpPath;
                this.root = (Node) DBApp.readObject(tmpPath);



            }
        }
        else
        {

            NonLeaf parent = (NonLeaf) DBApp.readObject(currentNode.parent);

            LeftAndRightSiblings lrs = getSibLings(pathToNode, parent);
            String siblingLeft = lrs.sibLingLeft;
            String siblingRight = lrs.sibLingRight;
            int parentIdx = lrs.idx;
            NonLeaf leftNonLeaf = null;
            NonLeaf rightNonLeaf = null;
            if(siblingLeft != null)
            {

                // borrow from left
                leftNonLeaf = (NonLeaf) DBApp.readObject(siblingLeft);

                if(leftNonLeaf.entries.size() > this.minNonLeaf)
                {
//

                    currentNode.borrow(leftNonLeaf, parent, true, parentIdx, tmpPath);
                    DBApp.writeObject(leftNonLeaf, siblingLeft);
                }
                else if(siblingRight != null)
                {


                    rightNonLeaf = (NonLeaf) DBApp.readObject(siblingRight);
                    if(rightNonLeaf.entries.size() > this.minNonLeaf)
                    {
                        currentNode.borrow(rightNonLeaf, parent, false, parentIdx, tmpPath);
                        DBApp.writeObject(rightNonLeaf, siblingRight);
                    }
                    else
                    {
                        currentNode.mergeWithNonLeaf(this, leftNonLeaf, parent, true, parentIdx, tmpPath);
                        DBApp.writeObject(leftNonLeaf, siblingLeft);
                    }
                }
                else
                {



                    currentNode.mergeWithNonLeaf(this, leftNonLeaf, parent, true, parentIdx, tmpPath);



                    DBApp.writeObject(leftNonLeaf, siblingLeft);

                }

            }
            else if(siblingRight != null)
            {
                // borrow from right
                rightNonLeaf = (NonLeaf) DBApp.readObject(siblingRight);
                if(rightNonLeaf.entries.size() > rightNonLeaf.min)
                {
                    currentNode.borrow(rightNonLeaf, parent, false, parentIdx, tmpPath);
                    DBApp.writeObject(rightNonLeaf, siblingRight);
                }
                else
                {
                    currentNode.mergeWithNonLeaf(this, rightNonLeaf, parent, false, parentIdx, tmpPath);
                    DBApp.writeObject(rightNonLeaf, siblingRight);
                }
            }
            else
            {

            }
            DBApp.writeObject(currentNode, pathToNode);
            DBApp.writeObject(parent, currentNode.parent);
        }
    }


    private static LeftAndRightSiblings getSibLings(String pathToNode, NonLeaf parent)
    {
        String siblingLeft = null;
        String siblingRight = null;
        int parentIdx = -1;
        for(int i=0; i<parent.entries.size(); i++)
        {
            NodeEntry e = parent.entries.get(i);
            if(e.left.equals(pathToNode)){
                parentIdx = i;
                if(i > 0)
                    siblingLeft = parent.entries.get(i-1).left;
                siblingRight = e.right;
                break;
            }
        }

        if(parent.entries.get(parent.entries.size()-1).right.equals(pathToNode)){

            parentIdx = parent.entries.size();
            siblingLeft = parent.entries.get(parent.entries.size()-1).left;
        }

        return new LeftAndRightSiblings(siblingLeft, siblingRight, parentIdx);
    }



    static class LeftAndRightSiblings{
        String sibLingLeft;
        String sibLingRight;
        int idx;

        public LeftAndRightSiblings(String left, String right, int i){
            this.sibLingLeft = left;
            this.sibLingRight = right;
            this.idx = i;

        }
    }

    protected void updateUpper(Object oldKey, Object newKey ,String pathToNode) throws ClassNotFoundException, IOException
    {
        if(pathToNode == null)
            return;
        NonLeaf nextNode = (NonLeaf) DBApp.readObject(pathToNode);

        for(NodeEntry e: nextNode.entries){
            if(DBApp.compare(e.key,oldKey) == 0)
            {
                e.key = newKey;
                DBApp.writeObject(nextNode, pathToNode);
                return;
            }
        }

        updateUpper(oldKey,newKey, nextNode.parent);
    }



    public TuplePointer find(Object key) throws ClassNotFoundException, IOException{
        Node root = (Node)DBApp.readObject(this.rootPath);
        if(root instanceof Leaf)
        {

            Leaf r = (Leaf) root;
            for(int i = 0; i<r.pointers.size(); i++)
            {
                Object k = r.pointers.get(i).key;
                if(DBApp.compare(k,key) == 0)
                {
                    return r.pointers.get(i);
                }
            }
        }
        else
        {
            Leaf leaf = (Leaf)DBApp.readObject(findLeaf(rootPath,key,null));
            for(int i=0; i<leaf.pointers.size(); i++){
                if(DBApp.compare(leaf.pointers.get(i).key,key) == 0)
                {
                    return leaf.pointers.get(i);
                }
                else if(DBApp.compare(leaf.pointers.get(i).key,key) > 0)
                {
                    break;
                }
            }
        }
        return null;
    }

    public String findNearestPage(Polygon key) throws ClassNotFoundException, IOException{
        Leaf r = (Leaf) DBApp.readObject(findLeaf(rootPath,key,null));
        int i = r.pointers.size()-1;
        while(i>=0 && DBApp.compare(r.pointers.get(i).key,key) > 0)
            i--;
        if (i != -1) {
            Overflow o = (Overflow) DBApp.readObject(r.pointers.get(i).overflowPath);
            return (String) o.pagesAndOccurrences.get(o.pagesAndOccurrences.size()-1).get("name");
        }
        else {
            if (r.prevLeafPath != null) {
                Leaf prev = (Leaf) DBApp.readObject(r.prevLeafPath);
                Overflow oPrev = (Overflow) DBApp.readObject(prev.pointers.get(prev.pointers.size()-1).overflowPath);
                return (String) oPrev.pagesAndOccurrences.get(oPrev.pagesAndOccurrences.size()-1).get("name");
            }
            return null;
        }
    }

    private String findLeaf(String current, Object key, String parent) throws ClassNotFoundException, IOException
    {
        Node cur = (Node) DBApp.readObject(current);
        cur.parent = parent;
        DBApp.writeObject(cur, current);
        if(cur instanceof Leaf)
        {
            return current;
        }

        NonLeaf curr = (NonLeaf) cur;
        String path= "";
        for(int i = 0; i<curr.entries.size(); i++)
        {
            if(DBApp.compare(curr.entries.get(i).key,key) > 0){
                path = curr.entries.get(i).left;
                break;
            }
            else if(i==curr.entries.size()-1){
                path = curr.entries.get(i).right;
            }

        }

        return findLeaf(path, key, current);



    }


    public String toString(){
        try {
            return printTree(rootPath);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return "ERROR";

        }
    }

    private static int getArea(Object polygon) {
        return ((Polygon) polygon).getBounds().width * ((Polygon) polygon).getBounds().height;
    }

    private String printTree(String path) throws ClassNotFoundException, IOException{

        Node n = (Node) DBApp.readObject(path);
        if(n instanceof Leaf)
        {
            String res = "";
            Leaf l = (Leaf) n;
            res+= "LEAF: ";
            for(TuplePointer tp : l.pointers)
            {
                res += getArea(tp.key)+" ";
            }

            res+="\n";
            return res;
        }
        NonLeaf nl = (NonLeaf) n;

        String res = "";
        for(NodeEntry e : nl.entries)
        {
            res += getArea(e.key)+" ";
        }
        res += "\n";

        res+= printTree(nl.entries.get(0).left);
        for(NodeEntry e : nl.entries)
        {
            res +=  printTree(e.right);
        }


        return res;
    }

    public static ArrayList<String> getEqual(String tableName, String colName, Object key) throws IOException, ClassNotFoundException {  //Created by Helmy
        ArrayList<String> pages = new ArrayList<>();
        String RtreePath = "data/" + tableName + "/indices/" + colName + "/Rtree.class";
        RTree tree = (RTree) DBApp.readObject(RtreePath);
        if (tree.find(key) != null) {
            Overflow overflow = (Overflow) DBApp.readObject(tree.find(key).overflowPath);
            for (Hashtable<String,Object> h : overflow.pagesAndOccurrences)
                pages.add((String) h.get("name"));
            overflow = null;
        }
        tree = null;
        return pages;
    }

    public static ArrayList<String> getLessThan(String tableName, String colName, Object key) throws IOException, ClassNotFoundException {  //Created by Helmy
        ArrayList<String> pages = new ArrayList<>();
        String RtreePath = "data/" + tableName + "/indices/" + colName + "/Rtree.class";
        RTree tree = (RTree) DBApp.readObject(RtreePath);
        String pathToLeaf = tree.findLeaf(tree.rootPath,key,null);
        Leaf leaf = (Leaf) DBApp.readObject(pathToLeaf);
        for (int i=0; i<leaf.pointers.size(); i++) {
            if (DBApp.compare(leaf.pointers.get(i).key,key) < 0) {
                Overflow overflow = (Overflow) DBApp.readObject(leaf.pointers.get(i).overflowPath);
                for (Hashtable<String,Object> h : overflow.pagesAndOccurrences) {
                    if (!pages.contains((String) h.get("name")))
                        pages.add((String) h.get("name"));
                }
                overflow = null;
            }
        }
        String strPrevLeaf = leaf.prevLeafPath;
        while (strPrevLeaf != null) {
            Leaf prevLeaf = (Leaf) DBApp.readObject(strPrevLeaf);
            for (int k=prevLeaf.pointers.size()-1; k>=0; k--) {
                Overflow overflow1 = (Overflow) DBApp.readObject(prevLeaf.pointers.get(k).overflowPath);
                for (Hashtable<String,Object> h1 : overflow1.pagesAndOccurrences) {
                    if (!pages.contains((String) h1.get("name")))
                        pages.add((String) h1.get("name"));
                }
                overflow1 = null;
            }
            strPrevLeaf = prevLeaf.prevLeafPath;
            prevLeaf = null;
        }
        tree = null;
        leaf =null;
        return pages;
    }

    public static ArrayList<String> getGreaterThan(String tableName, String colName, Object key) throws IOException, ClassNotFoundException {  //Created by Helmy
        ArrayList<String> pages = new ArrayList<>();
        String RtreePath = "data/" + tableName + "/indices/" + colName + "/Rtree.class";
        RTree tree = (RTree) DBApp.readObject(RtreePath);
        String pathToLeaf = tree.findLeaf(tree.rootPath,key,null);
        Leaf leaf = (Leaf) DBApp.readObject(pathToLeaf);
        for (int i=0; i<leaf.pointers.size(); i++) {
            if (DBApp.compare(leaf.pointers.get(i).key,key) > 0) {
                Overflow overflow = (Overflow) DBApp.readObject(leaf.pointers.get(i).overflowPath);
                for (Hashtable<String,Object> h : overflow.pagesAndOccurrences) {
                    if (!pages.contains((String) h.get("name")))
                        pages.add((String) h.get("name"));
                }
                overflow = null;
            }
        }
        String strNextLeaf = leaf.nextLeafPath;
        while (strNextLeaf != null) {
            Leaf nextLeaf = (Leaf) DBApp.readObject(strNextLeaf);
            for (int k=0; k<nextLeaf.pointers.size(); k++) {
                Overflow overflow1 = (Overflow) DBApp.readObject(nextLeaf.pointers.get(k).overflowPath);
                for (Hashtable<String,Object> h1 : overflow1.pagesAndOccurrences) {
                    if (!pages.contains((String) h1.get("name")))
                        pages.add((String) h1.get("name"));
                }
                overflow1 = null;
            }
            strNextLeaf = nextLeaf.nextLeafPath;
            nextLeaf = null;
        }
        tree = null;
        leaf =null;
        return pages;
    }

    public static ArrayList<String> getNotEqual(String tableName, String colName, Object key) throws IOException, ClassNotFoundException {  //Created by Helmy
        return getUnionPages(getLessThan(tableName,colName,key),getGreaterThan(tableName,colName,key));
    }

    public static ArrayList<String> getLessThanEqual(String tableName, String colName, Object key) throws IOException, ClassNotFoundException {  //Created by Helmy
        return getUnionPages(getLessThan(tableName,colName,key),getEqual(tableName,colName,key));
    }

    public static ArrayList<String> getGreaterThanEqual(String tableName, String colName, Object key) throws IOException, ClassNotFoundException {  //Created by Helmy
        return getUnionPages(getGreaterThan(tableName,colName,key),getEqual(tableName,colName,key));
    }

    public static ArrayList<String> getUnionPages(ArrayList<String> pages1, ArrayList<String> pages2) {  //Created by Helmy
        Set<String> set = new HashSet<>();
        set.addAll(pages1);
        set.addAll(pages2);
        return new ArrayList<>(set);
    }

    public static ArrayList<String> getIntersectionPages(ArrayList<String> pages1, ArrayList<String> pages2) {  //Created by Helmy
        ArrayList<String> result = new ArrayList<>();
        for (String page : pages1) {
            if (pages2.contains(page))
                result.add(page);
        }
        return result;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {

    }
}
