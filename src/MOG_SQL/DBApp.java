package MOG_SQL;

import java.awt.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class DBAppException extends Exception {
    DBAppException(String message) {
        super(message);
    }
}

class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;
}

class PolygonComparable implements Comparable {
    Polygon plg;

    PolygonComparable(Polygon p) {
        plg = p;
    }

    @Override
    public int compareTo(Object o) {
        Polygon oPlg = (Polygon) o;
        Dimension dim1 = this.plg.getBounds().getSize();
        Dimension dim2 = oPlg.getBounds().getSize();
        int area1 = dim1.width * dim1.height;
        int area2 = dim2.width * dim2.height;
        return Integer.compare(area1,area2);
    }
}

class Page implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;
    Vector<Hashtable<String,Object>> rows;

    Page() {
        rows = new Vector<>();
    }
}

class Table implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;
    String tableName;
    int nOfColumns;
    int pageID;
    ArrayList<String> pageNameArr;
    ArrayList<HashMap<Object,Object>> minMaxArr;
    ArrayList<String> indices;

    Table(String strTableName, int nCol) {
        tableName = strTableName;
        nOfColumns = nCol;
        pageID = 1;
        pageNameArr = new ArrayList<>();
        minMaxArr = new ArrayList<>();
        indices = new ArrayList<>();
    }
}

public class DBApp {

    int maximumRowsCountinPage;
    int nodeSize;

    public static String getClusterCol(String tablename) {
        String columnName = "";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(tablename) && data[3].equals("True")) {
                    columnName = data[1];
                }
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return columnName;
    }

    public static String getClusterColType(String tablename) {
        String columnName = "";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(tablename) && data[3].equals("True")) {
                    columnName = data[2];
                }
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return columnName;
    }

    public static String getColType(String tablename, String colname) {
        String columnType = "";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(tablename) && data[1].equals(colname)) {
                    columnType = data[2];
                }
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return columnType;
    }

    public static void createPage(Page page, String path, int id) {
        try {
            String pageName = path + "/page" + id + ".class";
            FileOutputStream fileOut = new FileOutputStream(pageName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Page loadPage(String path) {
        Page p = null;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return p;
    }

    public static int getnOfRows(String path) {
        Page p = loadPage(path);
        int n = p.rows.size();
        p = null;  //to eliminate
        return n;
    }

    public static int getMaxRows() {
        int n = 0;
        try {
            FileReader reader = new FileReader("config/DBApp.properties");
            Properties p = new Properties();
            p.load(reader);
            n = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return n;
    }

    public static int getNodeSize() {
        int n = 0;
        try {
            FileReader reader = new FileReader("config/DBApp.properties");
            Properties p = new Properties();
            p.load(reader);
            n = Integer.parseInt(p.getProperty("NodeSize"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return n;
    }

    public static void save(Object o, String path) {
        try {
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(o);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table loadTableInfo(String path) {
        Table t = null;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return t;
    }

    public static boolean tableExist(String strTableName) {
        boolean flag = false;
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(strTableName)) {
                    flag = true;
                    break;
                }
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public static void writeToMeta(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType) {

        try {
            FileWriter csvWriter = new FileWriter("data/metadata.csv",true);
            htblColNameType.forEach((colName, colType) -> {
                try {
                    csvWriter.append("\n");
                    csvWriter.append(strTableName).append(", ");
                    csvWriter.append(colName).append(", ");
                    csvWriter.append(colType).append(", ");
                    if (strClusteringKeyColumn.equals(colName))
                        csvWriter.append("True, ");
                    else
                        csvWriter.append("False, ");
                    csvWriter.append("False");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int nOfColumns(String tableName) {
        String tableInfoPath = "data/" + tableName + "/tableInfo.class";
        Table t = loadTableInfo(tableInfoPath);
        int n = t.nOfColumns;
        t = null;  //to eliminate
        return n;
    }

    public static boolean columnsContentCorrect(String tableName, Hashtable<String,Object> htblColNameValue) throws IOException {
        int count = 0;
        int nOfColumns = nOfColumns(tableName);
        Set<Map.Entry<String,Object>> entrySet = htblColNameValue.entrySet();
        for (Map.Entry<String,Object> entry : entrySet) {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(tableName) && data[1].equals(entry.getKey()) && data[2].equals(entry.getValue().getClass().getName())) {
                    count++;
                    break;
                }
            }
            csvReader.close();
        }
        return count == nOfColumns;
    }

    public static boolean columnsContentCorrectPartial(String tableName, Hashtable<String,Object> htblColNameValue) throws IOException {
        Set<Map.Entry<String,Object>> entrySet = htblColNameValue.entrySet();
        for (Map.Entry<String,Object> entry : entrySet) {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            boolean entryCorrect = false;
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(tableName) && data[1].equals(entry.getKey()) && data[2].equals(entry.getValue().getClass().getName())) {
                    entryCorrect = true;
                    break;
                }
            }
            if (!entryCorrect)
                return false;
            csvReader.close();
        }
        return true;
    }

    public static boolean termContentCorrect(SQLTerm term) throws IOException {
        BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
        boolean termCorrect = false;
        String row;
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(", ");
            if (data[0].equals(term._strTableName) && data[1].equals(term._strColumnName) && data[2].equals(term._objValue.getClass().getName())) {
                termCorrect = true;
                break;
            }
        }
        csvReader.close();
        return termCorrect;
    }

    public static void insertionSort(Vector<Hashtable<String,Object>> rows, String clusterCol) {
        for (int i=1; i<rows.size(); i++) {
            Hashtable<String,Object> key = rows.get(i);
            int j = i - 1;
            while ((j >= 0) && (compare(rows.get(j).get(clusterCol),key.get(clusterCol)) > 0)) {
                rows.add(j+1,rows.get(j));
                rows.remove(j+2);
                j = j - 1;
            }
            rows.add(j+1,key);
            rows.remove(j+2);
        }
    }

    public static int compare(Object value, Object o) {
        String objType = value.getClass().getName();
        int out = 0;
        switch (objType) {
            case "java.lang.Integer" :
                Integer valInt = (Integer) value;
                Integer oInt = (Integer) o;
                out = valInt.compareTo(oInt);
                break;
            case "java.lang.String" :
                String valStr = (String) value;
                String oStr = (String) o;
                out = valStr.compareTo(oStr);
                break;
            case "java.lang.Double" :
                Double valDbl = (Double) value;
                Double oDbl = (Double) o;
                out = valDbl.compareTo(oDbl);
                break;
            case "java.lang.Boolean" :
                Boolean valBln = (Boolean) value;
                Boolean oBln = (Boolean) o;
                out = valBln.compareTo(oBln);
                break;
            case "java.util.Date" :
                Date valDat = (Date) value;
                Date oDat = (Date) o;
                out = valDat.compareTo(oDat);
                break;
            case "java.awt.Polygon" :
                Polygon valPlg = (Polygon) value;
                PolygonComparable valPlgC = new PolygonComparable(valPlg);
                Polygon oPlg = (Polygon) o;
                out = valPlgC.compareTo(oPlg);
                break;
        }
        return out;
    }

    public static boolean polygonEquals(Polygon p1, Polygon p2) {
        return Arrays.equals(p1.xpoints,p2.xpoints) && Arrays.equals(p1.ypoints,p2.ypoints);
    }

    public static ArrayList<Integer> getContainPageIndex(String strTableName, String strClusteringKey) {
        String colType = getClusterColType(strTableName);
        Table t = loadTableInfo("data/" + strTableName + "/tableInfo.class");
        boolean flag = false;
        ArrayList<Integer> output = new ArrayList<>();
        switch (colType) {
            case "java.lang.Integer" :
                int keyInt = Integer.parseInt(strClusteringKey);
                for (int i=0 ; i<t.minMaxArr.size(); i++) {
                    Map<Object,Object> pMap = t.minMaxArr.get(i);
                    Map.Entry<Object,Object> p = pMap.entrySet().iterator().next();
                    if ((compare(keyInt,p.getKey()) >= 0) && (compare(keyInt,p.getValue()) <= 0)) {
                        flag = true;
                        output.add(i);
                    }
                    else {
                        if (flag)
                            break;
                    }
                }
                break;
            case "java.lang.String" :
                for (int i=0 ; i<t.minMaxArr.size(); i++) {
                    Map<Object,Object> pMap = t.minMaxArr.get(i);
                    Map.Entry<Object,Object> p = pMap.entrySet().iterator().next();
                    if ((compare(strClusteringKey,p.getKey()) >= 0) && (compare(strClusteringKey,p.getValue()) <= 0)) {
                        flag = true;
                        output.add(i);
                    }
                    else {
                        if (flag)
                            break;
                    }
                }
                break;
            case "java.lang.Double" :
                Double keyDbl = Double.parseDouble(strClusteringKey);
                for (int i=0 ; i<t.minMaxArr.size(); i++) {
                    Map<Object,Object> pMap = t.minMaxArr.get(i);
                    Map.Entry<Object,Object> p = pMap.entrySet().iterator().next();
                    if ((compare(keyDbl,p.getKey()) >= 0) && (compare(keyDbl,p.getValue()) <= 0)) {
                        flag = true;
                        output.add(i);
                    }
                    else {
                        if (flag)
                            break;
                    }
                }
                break;
            case "java.lang.Boolean" :
                Boolean keyBln = Boolean.parseBoolean(strClusteringKey);
                for (int i=0 ; i<t.minMaxArr.size(); i++) {
                    Map<Object,Object> pMap = t.minMaxArr.get(i);
                    Map.Entry<Object,Object> p = pMap.entrySet().iterator().next();
                    if ((compare(keyBln,p.getKey()) >= 0) && (compare(keyBln,p.getValue()) <= 0)) {
                        flag = true;
                        output.add(i);
                    }
                    else {
                        if (flag)
                            break;
                    }
                }
                break;
            case "java.util.Date" :
                try {
                    Date keyDate = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
                    for (int i=0 ; i<t.minMaxArr.size(); i++) {
                        Map<Object,Object> pMap = t.minMaxArr.get(i);
                        Map.Entry<Object,Object> p = pMap.entrySet().iterator().next();
                        if ((compare(keyDate,p.getKey()) >= 0) && (compare(keyDate,p.getValue()) <= 0)) {
                            flag = true;
                            output.add(i);
                        }
                        else {
                            if (flag)
                                break;
                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            case "java.awt.Polygon" :
                Polygon keyPlg = stringToPolygon(strClusteringKey);
                for (int i=0 ; i<t.minMaxArr.size(); i++) {
                    Map<Object,Object> pMap = t.minMaxArr.get(i);
                    Map.Entry<Object,Object> p = pMap.entrySet().iterator().next();
                    if ((compare(keyPlg,p.getKey()) >= 0) && (compare(keyPlg,p.getValue()) <= 0)) {
                        flag = true;
                        output.add(i);
                    }
                    else {
                        if (flag)
                            break;
                    }
                }
                break;
        }
        t = null;
        return output;
    }

    public static Polygon stringToPolygon(String s) {
        Polygon p = new Polygon();
        StringTokenizer st = new StringTokenizer(s,"(,)");
        ArrayList<Integer> a = new ArrayList<>();
        while (st.hasMoreTokens())
            a.add(Integer.parseInt(st.nextToken()));
        for (int i=0; i<a.size(); i++) {
            if ((i%2) == 0) {
                p.addPoint(a.get(i),a.get(i+1));
            }
        }
        return p;
    }

    public static int getIndexBinSearch(String strTableName,String strClusteringKey, int pageIndex) {
        String colName = getClusterCol(strTableName);
        String colType = getClusterColType(strTableName);
        Table t = loadTableInfo("data/" + strTableName + "/tableInfo.class");
        String pageName = t.pageNameArr.get(pageIndex);
        String pagePath = "data/" + strTableName + "/" + pageName;
        Page p = loadPage(pagePath);
        int first = 0;
        int last = p.rows.size() - 1;
        int mid = (first + last) / 2;
        switch (colType) {
            case "java.lang.Integer" :
                int keyInt = Integer.parseInt(strClusteringKey);
                while( first <= last ){
                    Integer midInt = (Integer) p.rows.get(mid).get(colName);
                    if (compare(midInt,keyInt) < 0){
                        first = mid + 1;
                    }else if (compare(midInt,keyInt) == 0){
                        return mid;
                    }else{
                        last = mid - 1;
                    }
                    mid = (first + last)/2;
                }
                break;
            case "java.lang.String" :
                while( first <= last ){
                    String midStr = (String) p.rows.get(mid).get(colName);
                    if (compare(midStr,strClusteringKey) < 0){
                        first = mid + 1;
                    }else if (compare(midStr,strClusteringKey) == 0){
                        return mid;
                    }else{
                        last = mid - 1;
                    }
                    mid = (first + last)/2;
                }
                break;
            case "java.lang.Double" :
                Double keyDbl = Double.parseDouble(strClusteringKey);
                while( first <= last ){
                    Double midDbl = (Double) p.rows.get(mid).get(colName);
                    if (compare(midDbl,keyDbl) < 0){
                        first = mid + 1;
                    }else if (compare(midDbl,keyDbl) == 0){
                        return mid;
                    }else{
                        last = mid - 1;
                    }
                    mid = (first + last)/2;
                }
                break;
            case "java.lang.Boolean" :
                Boolean keyBln = Boolean.parseBoolean(strClusteringKey);
                while( first <= last ){
                    Boolean midBln = (Boolean) p.rows.get(mid).get(colName);
                    if (compare(midBln,keyBln) < 0){
                        first = mid + 1;
                    }else if (compare(midBln,keyBln) == 0){
                        return mid;
                    }else{
                        last = mid - 1;
                    }
                    mid = (first + last)/2;
                }
                break;
            case "java.util.Date" :
                try {
                    Date keyDate = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
                    while( first <= last ){
                        Date midDate = (Date) p.rows.get(mid).get(colName);
                        if (compare(midDate,keyDate) < 0){
                            first = mid + 1;
                        }else if (compare(midDate,keyDate) == 0){
                            return mid;
                        }else{
                            last = mid - 1;
                        }
                        mid = (first + last)/2;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            case "java.awt.Polygon" :
                Polygon keyPlg = stringToPolygon(strClusteringKey);
                while( first <= last ){
                    Polygon midPlg = (Polygon) p.rows.get(mid).get(colName);
                    if (compare(midPlg,keyPlg) < 0){
                        first = mid + 1;
                    }else if (compare(midPlg,keyPlg) == 0){
                        return mid;
                    }else{
                        last = mid - 1;
                    }
                    mid = (first + last)/2;
                }
                break;
        }
        t = null;
        p = null;
        return 0;
    }

    public static Object readObject(String path) throws IOException, ClassNotFoundException{
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(path)));
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    public static void writeObject(Object x,String path) throws FileNotFoundException, IOException{
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(path)));
        oos.writeObject(x);
        oos.close();
    }


    public void init() {
        maximumRowsCountinPage = getMaxRows();
        nodeSize = getNodeSize();
    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType) throws DBAppException {

        if (tableExist(strTableName))
            throw new DBAppException("There is a table already with that name.");
        else {
            boolean clusterInHash = false;
            for (String key : htblColNameType.keySet()) {
                if (strClusteringKeyColumn.equals(key)) {
                    clusterInHash = true;
                    break;
                }
            }
            if (!clusterInHash)
                throw new DBAppException("Clustering key provided is not in htblColNameType.");
            Table t = new Table(strTableName, htblColNameType.size());
            writeToMeta(strTableName, strClusteringKeyColumn, htblColNameType);
            String directoryPath = "data/" + strTableName;
            String tableInfoPath = "data/" + strTableName + "/tableInfo.class";
            new File(directoryPath).mkdirs();
            new File("data/"+strTableName+"/indices").mkdir();
            save(t, tableInfoPath);
        }
    }

    public void createBTreeIndex(String strTableName,
                                 String strColName) throws DBAppException, IOException, ClassNotFoundException {
        if (!tableExist(strTableName))
            throw new DBAppException("This table does not exist.");
        else {
            if (checkIndexExists(strTableName,strColName))
                throw new DBAppException("Index already exists on this column.");
            Table table = loadTableInfo("data/" + strTableName + "/tableInfo.class");
            BPTree tree = new BPTree(nodeSize,strTableName,strColName);
            table.indices.add(strColName);
            for(String pageName: table.pageNameArr)
            {
                Page page = loadPage("data/" + strTableName + "/" + pageName);
                for(int i=0; i<page.rows.size(); i++)
                {
                    tree.insert(page.rows.get(i).get(strColName), pageName);
                }
                page = null;
            }
            updateMetaIndex(strTableName,strColName);
            save(table,"data/" + strTableName + "/tableInfo.class");
            table = null;
            tree = null;
        }
    }

    public void createRTreeIndex(String strTableName,
                                 String strColName) throws DBAppException, IOException, ClassNotFoundException {
        if (!tableExist(strTableName))
            throw new DBAppException("This table does not exist.");
        else {
            if (checkIndexExists(strTableName,strColName))
                throw new DBAppException("Index already exists on this column.");
            Table table = loadTableInfo("data/" + strTableName + "/tableInfo.class");
            RTree tree = new RTree(nodeSize,strTableName,strColName);
            table.indices.add(strColName);
            for(String pageName: table.pageNameArr)
            {
                Page page = loadPage("data/" + strTableName + "/" + pageName);
                for(int i=0; i<page.rows.size(); i++)
                {
                    tree.insert(page.rows.get(i).get(strColName), pageName);
                }
                page = null;
            }
            updateMetaIndex(strTableName,strColName);
            save(table,"data/" + strTableName + "/tableInfo.class");
            table = null;
            tree = null;
        }
    }

    private boolean checkIndexExists(String strTableName, String strColName) {
        File f;
        if (getColType(strTableName, strColName).equals("java.awt.Polygon"))
            f = new File("data/"+strTableName+"/indices/"+strColName+"/RTree.class");
        else
            f = new File("data/"+strTableName+"/indices/"+strColName+"/BTree.class");
        return f.exists();
    }

    private void updateMetaIndex(String strTableName, String strColName) {
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            FileWriter csvWriterTemp = new FileWriter("data/metadataTemp.csv",true);
            String row;
            String beginning = "Table Name, Column Name, Column Type, ClusteringKey, Indexed";
            csvWriterTemp.append(beginning);
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                csvWriterTemp.append("\n");
                if (data[0].equals(strTableName) && data[1].equals(strColName)) {
                    StringBuffer sb = new StringBuffer(row);
                    sb.replace(row.length()-5,row.length(),"True");
                    csvWriterTemp.append(sb);
                }
                else
                    csvWriterTemp.append(row);
            }
            csvReader.close();
            csvWriterTemp.flush();
            csvWriterTemp.close();
            File file = new File("data/metadata.csv");
            file.delete();
            updateMetaIndexHelper();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateMetaIndexHelper() {
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadataTemp.csv"));
            FileWriter csvWriter = new FileWriter("data/metadata.csv",true);
            String row;
            String beginning = "Table Name, Column Name, Column Type, ClusteringKey, Indexed";
            csvWriter.append(beginning);
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                csvWriter.append("\n");
                csvWriter.append(row);
            }
            csvReader.close();
            csvWriter.flush();
            csvWriter.close();
            File file = new File("data/metadataTemp.csv");
            file.delete();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        if (!tableExist(strTableName))
            throw new DBAppException("This table doest not exist.");
        else if (!columnsContentCorrect(strTableName,htblColNameValue))
            throw new DBAppException("Given columns' content is not correct.");
        else {
            String clusterCol = getClusterCol(strTableName);
            Object clusterVal = htblColNameValue.get(clusterCol);
            String directoryPath = "data/" + strTableName;
            String tableInfoPath = "data/" + strTableName + "/tableInfo.class";
            Table t = loadTableInfo(tableInfoPath);
            int nOfPages = t.pageNameArr.size();
            Date date = new Date();
            htblColNameValue.put("TouchDate",date);
            if (t.pageNameArr.size() == 0) {
                Page page = new Page();
                page.rows.add(htblColNameValue);
                createPage(page, directoryPath, t.pageID);
                String pageName = "page" + t.pageID + ".class";
                t.pageNameArr.add(pageName);
                t.pageID = t.pageID + 1;
                HashMap<Object,Object> pair = new HashMap<>();
                pair.put(clusterVal,clusterVal);
                t.minMaxArr.add(pair);
                for(String key : htblColNameValue.keySet())
                {
                    if(t.indices.contains(key))
                    {
                        if (getColType(strTableName,key).equals("java.awt.Polygon")) {
                            RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+key+"/Rtree.class");
                            tree.insert(htblColNameValue.get(key), pageName);
                            tree = null;
                        }
                        else {
                            BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+key+"/Btree.class");
                            tree.insert(htblColNameValue.get(key), pageName);
                            tree = null;
                        }
                    }
                }
                save(t, tableInfoPath);
            }
            else {
                if (t.indices.contains(clusterCol)) {
                    int index;
                    if (getColType(strTableName,clusterCol).equals("java.awt.Polygon")) {
                        RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+clusterCol+"/Rtree.class");
                        if (tree.find(clusterVal)!=null) {
                            String overflowPath = tree.find(clusterVal).overflowPath;
                            Overflow overflow = (Overflow) readObject(overflowPath);
                            String firstPageInIndex = (String) overflow.pagesAndOccurrences.get(0).get("name");
                            index = t.pageNameArr.indexOf(firstPageInIndex);
                            Map.Entry<Object,Object> minMax = t.minMaxArr.get(index).entrySet().iterator().next();
                            if ((compare(clusterVal,minMax.getKey()) == 0) && (index != 0)) {
                                Page prev = loadPage("data/" + strTableName + "/" + t.pageNameArr.get(index-1));
                                if (prev.rows.size() < maximumRowsCountinPage)
                                    index = index - 1;
                                prev = null;
                            }
                            overflow = null;
                            minMax = null;
                        }
                        else {
                            String nearestPage = tree.findNearestPage((Polygon) clusterVal);
                            if (nearestPage != null)
                                index = t.pageNameArr.indexOf(nearestPage);
                            else
                                index = 0;
                        }
                        tree = null;
                    }
                    else {
                        BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+clusterCol+"/Btree.class");
                        if (tree.find(clusterVal)!=null) {
                            String overflowPath = tree.find(clusterVal).overflowPath;
                            Overflow overflow = (Overflow) readObject(overflowPath);
                            String firstPageInIndex = (String) overflow.pagesAndOccurrences.get(0).get("name");
                            index = t.pageNameArr.indexOf(firstPageInIndex);
                            Map.Entry<Object,Object> minMax = t.minMaxArr.get(index).entrySet().iterator().next();
                            if ((compare(clusterVal,minMax.getKey()) == 0) && (index != 0)) {
                                Page prev = loadPage("data/" + strTableName + "/" + t.pageNameArr.get(index-1));
                                if (prev.rows.size() < maximumRowsCountinPage)
                                    index = index - 1;
                                prev = null;
                            }
                            overflow = null;
                        }
                        else {
                            String nearestPage = tree.findNearestPage(clusterVal);
                            if (nearestPage != null)
                                index = t.pageNameArr.indexOf(nearestPage);
                            else
                                index = 0;
                        }
                        tree = null;
                    }
                    insertHelper(strTableName, index, nOfPages-1, htblColNameValue);
                }
                else
                    insertHelper(strTableName, 0, nOfPages-1, htblColNameValue);
            }
            t = null;  //to eliminate
        }
    }

    public void insertHelper(String strTableName, int index, int last, Hashtable<String,Object> htblColNameValue) throws IOException, ClassNotFoundException {
        String clusterCol = getClusterCol(strTableName);
        Object clusterVal = htblColNameValue.get(clusterCol);
        String directoryPath = "data/" + strTableName;
        String tableInfoPath = "data/" + strTableName + "/tableInfo.class";
        Table t = loadTableInfo(tableInfoPath);
        Map<Object,Object> minMaxMap = t.minMaxArr.get(index);
        Map.Entry<Object,Object> minMax = minMaxMap.entrySet().iterator().next();
        String pageName = t.pageNameArr.get(index);
        String pagePath = "data/" + strTableName + "/" + pageName;
        int nOfRows = getnOfRows(pagePath);
        if (nOfRows == maximumRowsCountinPage) {
            if (compare(clusterVal,minMax.getValue()) <= 0) {  //if (value <= Max)
                Page p = loadPage(pagePath);
                Hashtable<String,Object> maxRow = p.rows.remove(nOfRows-1);
                p.rows.add(htblColNameValue);
                insertionSort(p.rows,clusterCol);
                HashMap<Object,Object> minMaxUpdated = new HashMap<>();
                minMaxUpdated.put(p.rows.get(0).get(clusterCol),p.rows.get(maximumRowsCountinPage-1).get(clusterCol));
                t.minMaxArr.add(index,minMaxUpdated);
                t.minMaxArr.remove(index+1);
                for(String key : htblColNameValue.keySet())
                {
                    if(t.indices.contains(key))
                    {
                        if (getColType(strTableName,key).equals("java.awt.Polygon")) {
                            RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+key+"/Rtree.class");
                            tree.delete(maxRow.get(key), pageName);
                            tree.insert(htblColNameValue.get(key), pageName);
                            tree = null;
                        }
                        else {
                            BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+key+"/Btree.class");
                            tree.delete(maxRow.get(key), pageName);
                            tree.insert(htblColNameValue.get(key), pageName);
                            tree = null;
                        }
                    }
                }
                save(p,pagePath);
                if (index == last) {
                    Page newPage = new Page();
                    newPage.rows.add(maxRow);
                    createPage(newPage, directoryPath, t.pageID);
                    String newPageName = "page" + t.pageID + ".class";
                    t.pageNameArr.add("page" + t.pageID + ".class");
                    t.pageID = t.pageID + 1;
                    Object maxVal = maxRow.get(clusterCol);
                    HashMap<Object,Object> pair = new HashMap<>();
                    pair.put(maxVal,maxVal);
                    t.minMaxArr.add(pair);
                    for(String key : htblColNameValue.keySet())
                    {
                        if(t.indices.contains(key))
                        {
                            if (getColType(strTableName,key).equals("java.awt.Polygon")) {
                                RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+key+"/Rtree.class");
                                tree.insert(maxRow.get(key), newPageName);
                                tree = null;
                            }
                            else {
                                BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+key+"/Btree.class");
                                tree.insert(maxRow.get(key), newPageName);
                                tree = null;
                            }
                        }
                    }
                    save(t,tableInfoPath);
                    t = null;  //to eliminate
                    p = null;  //to eliminate
                }
                else {
                    save(t,tableInfoPath);
                    t = null;  //to eliminate
                    p = null;  //to eliminate
                    insertHelper(strTableName,index+1,last,maxRow);
                }
            }
            else {
                if (index == last) {
                    Page newPage = new Page();
                    newPage.rows.add(htblColNameValue);
                    createPage(newPage, directoryPath, t.pageID);
                    String newPageName = "page" + t.pageID + ".class";
                    t.pageNameArr.add("page" + t.pageID + ".class");
                    t.pageID = t.pageID + 1;
                    HashMap<Object,Object> pair = new HashMap<>();
                    pair.put(clusterVal,clusterVal);
                    t.minMaxArr.add(pair);
                    for(String key : htblColNameValue.keySet())
                    {
                        if(t.indices.contains(key))
                        {
                            if (getColType(strTableName,key).equals("java.awt.Polygon")) {
                                RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+key+"/Rtree.class");
                                tree.insert(htblColNameValue.get(key), newPageName);
                                tree = null;
                            }
                            else {
                                BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+key+"/Btree.class");
                                tree.insert(htblColNameValue.get(key), newPageName);
                                tree = null;
                            }
                        }
                    }
                    save(t,tableInfoPath);
                    t = null;  //to eliminate
                }
                else {
                    t = null;  //to eliminate
                    insertHelper(strTableName,index+1,last,htblColNameValue);
                }
            }
        }
        else {
            if ((compare(clusterVal,minMax.getValue()) <= 0) || (index == last)) {
                Page p = loadPage(pagePath);
                p.rows.add(htblColNameValue);
                insertionSort(p.rows,clusterCol);
                HashMap<Object,Object> minMaxUpdated = new HashMap<>();
                minMaxUpdated.put(p.rows.get(0).get(clusterCol),p.rows.get(p.rows.size()-1).get(clusterCol));
                t.minMaxArr.add(index,minMaxUpdated);
                t.minMaxArr.remove(index+1);
                for(String key : htblColNameValue.keySet())
                {
                    if(t.indices.contains(key))
                    {
                        if (getColType(strTableName,key).equals("java.awt.Polygon")) {
                            RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+key+"/Rtree.class");
                            tree.insert(htblColNameValue.get(key), pageName);
                            tree = null;
                        }
                        else {
                            BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+key+"/Btree.class");
                            tree.insert(htblColNameValue.get(key), pageName);
                            tree = null;
                        }
                    }
                }
                save(p,pagePath);
                save(t,tableInfoPath);
                t = null;  //to eliminate
                p = null;  //to eliminate
            }
            else {
                Map<Object,Object> minMaxNextMap = t.minMaxArr.get(index+1);
                Map.Entry<Object,Object> minMaxNext = minMaxNextMap.entrySet().iterator().next();
                if ((compare(clusterVal,minMaxNext.getKey()) <= 0)) {
                    Page p = loadPage(pagePath);
                    p.rows.add(htblColNameValue);
                    insertionSort(p.rows,clusterCol);
                    HashMap<Object,Object> minMaxUpdated = new HashMap<>();
                    minMaxUpdated.put(p.rows.get(0).get(clusterCol),p.rows.get(p.rows.size()-1).get(clusterCol));
                    t.minMaxArr.add(index,minMaxUpdated);
                    t.minMaxArr.remove(index+1);
                    for(String key : htblColNameValue.keySet())
                    {
                        if(t.indices.contains(key))
                        {
                            if (getColType(strTableName,key).equals("java.awt.Polygon")) {
                                RTree tree = (RTree)readObject("data/"+strTableName+"/indices/"+key+"/Rtree.class");
                                tree.insert(htblColNameValue.get(key), pageName);
                                tree = null;
                            }
                            else {
                                BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+key+"/Btree.class");
                                tree.insert(htblColNameValue.get(key), pageName);
                                tree = null;
                            }
                        }
                    }
                    save(p,pagePath);
                    save(t,tableInfoPath);
                    t = null;  //to eliminate
                    p = null;  //to eliminate
                }
                else {
                    t = null;  //to eliminate
                    insertHelper(strTableName,index+1,last,htblColNameValue);
                }
            }
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKey,
                            Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        if(!tableExist(strTableName))
        {
            throw new DBAppException("This table does not exist.");
        }
        else {
            if (!columnsContentCorrectPartial(strTableName,htblColNameValue))
                throw new DBAppException("Given columns' content is not correct.");
            Table t = loadTableInfo("data/" + strTableName + "/tableInfo.class");
            String colName = getClusterCol(strTableName);
            String colType = getClusterColType(strTableName);
            ArrayList<Integer> pagesIndices;
            Date date = new Date();
            if (t.indices.contains(colName)) {
                Object key = null;
                switch (colType) {
                    case "java.lang.Integer" :
                        key = Integer.parseInt(strClusteringKey);
                        break;
                    case "java.lang.String" :
                        key = strClusteringKey;
                        break;
                    case "java.lang.Double" :
                        key = Double.parseDouble(strClusteringKey);
                        break;
                    case "java.lang.Boolean" :
                        key = Boolean.parseBoolean(strClusteringKey);
                        break;
                    case "java.util.Date" :
                        try {
                            key = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        break;
                    case "java.awt.Polygon" :
                        key = stringToPolygon(strClusteringKey);
                        break;
                    default:
                        key = null;
                }
                if (colType.equals("java.awt.Polygon")) {
                    RTree tree = (RTree) readObject("data/Student/indices/"+colName+"/Rtree.class");
                    Overflow overflow = (Overflow) readObject(tree.find(key).overflowPath);
                    ArrayList<String> pageNames = new ArrayList<>();
                    for (Hashtable<String,Object> h : overflow.pagesAndOccurrences)
                        pageNames.add((String) h.get("name"));
                    pagesIndices = getPagesIndices(pageNames,t.pageNameArr);
                    tree = null;
                    overflow = null;
                    pageNames = null;
                }
                else {
                    BPTree tree = (BPTree) readObject("data/Student/indices/"+colName+"/Btree.class");
                    Overflow overflow = (Overflow) readObject(tree.find(key).overflowPath);
                    ArrayList<String> pageNames = new ArrayList<>();
                    for (Hashtable<String,Object> h : overflow.pagesAndOccurrences)
                        pageNames.add((String) h.get("name"));
                    pagesIndices = getPagesIndices(pageNames,t.pageNameArr);
                    tree = null;
                    overflow = null;
                    pageNames = null;
                }
            }
            else
                pagesIndices = getContainPageIndex(strTableName,strClusteringKey);

            for (Integer index : pagesIndices) {
                String pageName = t.pageNameArr.get(index);
                String pagePath = "data/" + strTableName + "/" + pageName;
                Page p = loadPage(pagePath);
                int nOfRows = p.rows.size();
                int recordIndex1 = getIndexBinSearch(strTableName,strClusteringKey,index);
                int recordIndex2 = getIndexBinSearch(strTableName,strClusteringKey,index);
                Boolean flag1 = true;
                Boolean flag2 = true;
                while ((recordIndex1 >= 0) && flag1) {
                    if (colType.equals("java.awt.Polygon")) {
                        if (polygonEquals(stringToPolygon(strClusteringKey),(Polygon) p.rows.get(recordIndex1).get(colName))) {
                            for(String key1 : htblColNameValue.keySet())
                            {
                                if(t.indices.contains(key1))
                                {
                                    if (getColType(strTableName,key1).equals("java.awt.Polygon")) {
                                        RTree tree2 = (RTree)readObject("data/"+strTableName+"/indices/"+key1+"/Rtree.class");
                                        tree2.delete(p.rows.get(recordIndex1).get(key1),t.pageNameArr.get(index));
                                        tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                    }
                                    else {
                                        BPTree tree2 = (BPTree)readObject("data/"+strTableName+"/indices/"+key1+"/Btree.class");
                                        tree2.delete(p.rows.get(recordIndex1).get(key1),t.pageNameArr.get(index));
                                        tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                    }
                                }
                                p.rows.get(recordIndex1).replace(key1,htblColNameValue.get(key1));
                            }
                            p.rows.get(recordIndex1).replace("TouchDate",date);
                        }
                    }
                    else {
                        for(String key1 : htblColNameValue.keySet())
                        {
                            if(t.indices.contains(key1))
                            {
                                if (getColType(strTableName,key1).equals("java.awt.Polygon")) {
                                    RTree tree2 = (RTree)readObject("data/"+strTableName+"/indices/"+key1+"/Rtree.class");
                                    tree2.delete(p.rows.get(recordIndex1).get(key1),t.pageNameArr.get(index));
                                    tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                }
                                else {
                                    BPTree tree2 = (BPTree)readObject("data/"+strTableName+"/indices/"+key1+"/Btree.class");
                                    tree2.delete(p.rows.get(recordIndex1).get(key1),t.pageNameArr.get(index));
                                    tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                }
                            }
                            p.rows.get(recordIndex1).replace(key1,htblColNameValue.get(key1));
                        }
                        p.rows.get(recordIndex1).replace("TouchDate",date);
                    }
                    recordIndex1 = recordIndex1 - 1;
                    switch (colType) {
                        case "java.lang.Integer" :
                            if (recordIndex1 != -1) {
                                Integer keyInt = Integer.parseInt(strClusteringKey);
                                Integer recordInt = (Integer) p.rows.get(recordIndex1).get(colName);
                                if (compare(keyInt,recordInt) != 0)
                                    flag1 = false;
                            }
                            break;
                        case "java.lang.String" :
                            if (recordIndex1 != -1) {
                                String recordStr = (String) p.rows.get(recordIndex1).get(colName);
                                if (compare(strClusteringKey,recordStr) != 0)
                                    flag1 = false;
                            }
                            break;
                        case "java.lang.Double" :
                            if (recordIndex1 != -1) {
                                Double keyDbl = Double.parseDouble(strClusteringKey);
                                Double recordDbl = (Double) p.rows.get(recordIndex1).get(colName);
                                if (compare(keyDbl,recordDbl) != 0)
                                    flag1 = false;
                            }
                            break;
                        case "java.lang.Boolean" :
                            if (recordIndex1 != -1) {
                                Boolean keyBln = Boolean.parseBoolean(strClusteringKey);
                                Boolean recordBln = (Boolean) p.rows.get(recordIndex1).get(colName);
                                if (compare(keyBln,recordBln) != 0)
                                    flag1 = false;
                            }
                            break;
                        case "java.util.Date" :
                            try {
                                if (recordIndex1 != -1) {
                                    Date keyDate = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
                                    Date recordDate = (Date) p.rows.get(recordIndex1).get(colName);
                                    if (compare(keyDate,recordDate) != 0)
                                        flag1 = false;
                                }
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            break;
                        case "java.awt.Polygon" :
                            if (recordIndex1 != -1) {
                                Polygon keyPlg = stringToPolygon(strClusteringKey);
                                Polygon recordPlg = (Polygon) p.rows.get(recordIndex1).get(colName);
                                if (!polygonEquals(keyPlg,recordPlg))
                                    flag1 = false;
                            }
                            break;
                    }
                }
                while ((recordIndex2 < nOfRows) && flag2) {
                    if (colType.equals("java.awt.Polygon")) {
                        if (polygonEquals(stringToPolygon(strClusteringKey),(Polygon) p.rows.get(recordIndex2).get(colName))) {
                            for(String key1 : htblColNameValue.keySet())
                            {
                                if(t.indices.contains(key1))
                                {
                                    if (getColType(strTableName,key1).equals("java.awt.Polygon")) {
                                        RTree tree2 = (RTree)readObject("data/"+strTableName+"/indices/"+key1+"/Rtree.class");
                                        tree2.delete(p.rows.get(recordIndex2).get(key1),t.pageNameArr.get(index));
                                        tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                    }
                                    else {
                                        BPTree tree2 = (BPTree)readObject("data/"+strTableName+"/indices/"+key1+"/Btree.class");
                                        tree2.delete(p.rows.get(recordIndex2).get(key1),t.pageNameArr.get(index));
                                        tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                    }
                                }
                                p.rows.get(recordIndex2).replace(key1,htblColNameValue.get(key1));
                            }
                            p.rows.get(recordIndex2).replace("TouchDate",date);
                        }
                    }
                    else {
                        for(String key1 : htblColNameValue.keySet())
                        {
                            if(t.indices.contains(key1))
                            {
                                if (getColType(strTableName,key1).equals("java.awt.Polygon")) {
                                    RTree tree2 = (RTree)readObject("data/"+strTableName+"/indices/"+key1+"/Rtree.class");
                                    tree2.delete(p.rows.get(recordIndex2).get(key1),t.pageNameArr.get(index));
                                    tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                }
                                else {
                                    BPTree tree2 = (BPTree)readObject("data/"+strTableName+"/indices/"+key1+"/Btree.class");
                                    tree2.delete(p.rows.get(recordIndex2).get(key1),t.pageNameArr.get(index));
                                    tree2.insert(htblColNameValue.get(key1), t.pageNameArr.get(index));
                                }
                            }
                            p.rows.get(recordIndex2).replace(key1,htblColNameValue.get(key1));
                        }
                        p.rows.get(recordIndex2).replace("TouchDate",date);
                    }
                    recordIndex2 = recordIndex2 + 1;
                    switch (colType) {
                        case "java.lang.Integer" :
                            if (recordIndex2 != nOfRows) {
                                Integer keyInt = Integer.parseInt(strClusteringKey);
                                Integer recordInt = (Integer) p.rows.get(recordIndex2).get(colName);
                                if (compare(keyInt,recordInt) != 0)
                                    flag2 = false;
                            }
                            break;
                        case "java.lang.String" :
                            if (recordIndex2 != nOfRows) {
                                String recordStr = (String) p.rows.get(recordIndex2).get(colName);
                                if (compare(strClusteringKey,recordStr) != 0)
                                    flag2 = false;
                            }
                            break;
                        case "java.lang.Double" :
                            if (recordIndex2 != nOfRows) {
                                Double keyDbl = Double.parseDouble(strClusteringKey);
                                Double recordDbl = (Double) p.rows.get(recordIndex2).get(colName);
                                if (compare(keyDbl,recordDbl) != 0)
                                    flag2 = false;
                            }
                            break;
                        case "java.lang.Boolean" :
                            if (recordIndex2 != nOfRows) {
                                Boolean keyBln = Boolean.parseBoolean(strClusteringKey);
                                Boolean recordBln = (Boolean) p.rows.get(recordIndex2).get(colName);
                                if (compare(keyBln,recordBln) != 0)
                                    flag2 = false;
                            }
                            break;
                        case "java.util.Date" :
                            try {
                                if (recordIndex1 != -1) {
                                    Date keyDate = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
                                    Date recordDate = (Date) p.rows.get(recordIndex1).get(colName);
                                    if (compare(keyDate,recordDate) != 0)
                                        flag2 = false;
                                }
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            break;
                        case "java.awt.Polygon" :
                            if (recordIndex2 != nOfRows) {
                                Polygon keyPlg = stringToPolygon(strClusteringKey);
                                Polygon recordPlg = (Polygon) p.rows.get(recordIndex2).get(colName);
                                if (!polygonEquals(keyPlg,recordPlg))
                                    flag2 = false;
                            }
                            break;
                    }
                }
                save(p,pagePath);
                p = null;
            }
            t = null;
        }
    }

    public static ArrayList<Integer> getPagesIndices(ArrayList<String> indexPages, ArrayList<String> tablePages) {
        ArrayList<Integer> output = new ArrayList<>();
        for (String s : indexPages) {
            output.add(tablePages.indexOf(s));
        }
        return output;
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        Hashtable<String , Object> indices = new Hashtable<>();
        ArrayList<String> pagesInIndex = new ArrayList<String>();
        String keyIndex = "";
        boolean hasIndex = false;
        Object indexVal = null;
        Page p = null;
        Table t = null;
        Vector<Integer> helper = new Vector<>();
        Vector<Boolean> finalHelper = new Vector<>();
        if(!tableExist(strTableName))
        {
            throw new DBAppException("This table does not exist.");
        }
        if (!columnsContentCorrectPartial(strTableName,htblColNameValue)) {
            throw new DBAppException("Given columns' content is not correct.");
        }
        else
        {
            String clusterCol = getClusterCol(strTableName);
            String directoryPath = "data/" + strTableName;
            String tableInfoPath = "data/" + strTableName + "/tableInfo.class";
            t = loadTableInfo(tableInfoPath);
            for(int i = 0 ; i < t.pageNameArr.size() ; i++) finalHelper.add(i , false);
            for(String key : htblColNameValue.keySet())
            {
                if(t.indices.contains(key))
                {
                    Object value = htblColNameValue.get(key);
                    indices.put(key , value);
                    hasIndex = true;
                }
            }
            ArrayList<String> allIndices = getIndexKeys(strTableName);
            if(hasIndex)
            {
                ArrayList<String> pages = indicesIntersection(indices , strTableName);
                for(String pageName : pages)
                {
                    p = loadPage(directoryPath + "/" +pageName);
                    for(int i = 0; i < p.rows.size() ; i++)
                    {
                        if(hashSub(htblColNameValue , p.rows.get(i)))
                        {
                            for(String index : allIndices)
                            {
                                Object v = p.rows.get(i).get(index);          ///////////////////////////////////////
                                if(v instanceof Polygon)
                                {
                                    RTree indexRT = (RTree)readObject("data/"+strTableName+"/indices/"+index+"/Rtree.class");
                                    indexRT.delete(v, pageName);
                                    indexRT = null;
                                }
                                else {
                                    BPTree indexBT = (BPTree)readObject("data/"+strTableName+"/indices/"+index+"/Btree.class");
                                    indexBT.delete(v, pageName);
                                    indexBT = null;                                ////////////////////////////////////////
                                }
                            }
                            helper.add(i);
                        }
                    }
                    deleteHelper(tableInfoPath ,pageName,(directoryPath + "/" + pageName),clusterCol, helper , finalHelper);
                    helper.clear();
                    p = null;
                }
            }
            else
            {
                for(String pageName : t.pageNameArr)
                {
                    p = loadPage(directoryPath + "/" + pageName);
                    for(int i = 0; i < p.rows.size() ; i++)
                    {
                        if(hashSub(htblColNameValue , p.rows.get(i)))
                        {
                            for(String index : allIndices)
                            {
                                BPTree indexT = (BPTree)readObject("data/"+strTableName+"/indices/"+index+"/Btree.class");
                                Object v = p.rows.get(i).get(index);
                                indexT.delete(v, pageName);
                                indexT = null;
                            }
                            helper.add(i);
                        }
                    }
                    deleteHelper(tableInfoPath ,pageName,(directoryPath + "/" + pageName),clusterCol, helper , finalHelper);
                    helper.clear();
                    p = null;
                }
            }
            finishDeletion(directoryPath , finalHelper);
        }
        p = null;
        t = null;
    }

    public ArrayList<String> indicesIntersection(Hashtable<String,Object> h , String strTableName) throws IOException, ClassNotFoundException {
        ArrayList<String> result = new ArrayList<>();
        Iterator iter = h.keySet().iterator();
        while(iter.hasNext())
        {
            String currentKey =(String) iter.next();
            if(h.get(currentKey) instanceof Polygon)                                                          ////////////////////////////if and else for R and B
            {
                RTree treeR = (RTree)readObject("data/"+strTableName+"/indices/"+currentKey+"/Rtree.class");
                String overflowPath = treeR.find(h.get(currentKey)).overflowPath;
                Overflow overflowR = (Overflow) readObject(overflowPath);
                ArrayList<String> pagesR = new ArrayList<>();
                for(Hashtable<String , Object> htbl : overflowR.pagesAndOccurrences)
                {
                    pagesR.add((String)htbl.get("name"));
                }
                if (result.isEmpty())
                {
                    for(String s : pagesR)
                    {
                        result.add(s);
                    }
                }
                else{
                    for(String str : result)
                    {
                        if(!(pagesR.contains(str))) result.remove(str);
                    }
                }
            }                                                                                              //////////////////////////////////////////
            else {
                BPTree tree = (BPTree)readObject("data/"+strTableName+"/indices/"+currentKey+"/Btree.class");
                String overflowPath = tree.find(h.get(currentKey)).overflowPath;
                Overflow overflow = (Overflow) readObject(overflowPath);
                ArrayList<String> pages = new ArrayList<>();
                for(Hashtable<String , Object> htbl : overflow.pagesAndOccurrences)
                {
                    pages.add((String)htbl.get("name"));
                }
                if (result.isEmpty())
                {
                    for(String s : pages)
                    {
                        result.add(s);
                    }
                }
                else{
                    for(String str : result)
                    {
                        if(!(pages.contains(str))) result.remove(str);
                    }
                }
            }
        }
        return result;

    }


    public static ArrayList<String> getIndexKeys(String strTableName)
    {
        ArrayList<String> allIndices = new ArrayList<>();
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("data/metadata.csv"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(", ");
                if (data[0].equals(strTableName) && data[4].equals("True")) {
                    allIndices.add(data[1]);
                }
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return allIndices;
    }

    public static void deleteHelper(String tablePath,String pageName,String pagePath ,String key,
                                    Vector<Integer> v, Vector<Boolean> fv )
    {
        int tindex;
        int countD = 0;
        Object min;
        Object max;
        Page p = loadPage(pagePath);
        Table t = loadTableInfo(tablePath);
        tindex = t.pageNameArr.indexOf(pageName);
        for(Integer i : v)
        {
            p.rows.remove(i-countD);
            countD++;
            if(!p.rows.isEmpty()) {
                fv.add(tindex,false);
                min = p.rows.get(0).get(key);
                max = p.rows.get(p.rows.size() - 1).get(key);
                HashMap<Object,Object> minMax = new HashMap<>();
                minMax.put(min,max);
                t.minMaxArr.set(tindex, minMax);
                save(p, pagePath);
                save(t, tablePath);
            }
            else{
                fv.add(tindex,true);
            }

        }
        p = null;
    }

    public static void finishDeletion(String directory , Vector<Boolean> fv)
    {
        Table t = loadTableInfo(directory + "/tableInfo.class");
        int size = t.pageNameArr.size();
        for(int i = size - 1; i>=0; i--)
        {
            if(fv.get(i))
            {
                File f = new File(directory + "/" + t.pageNameArr.get(i));
                if(f.delete())
                {
                    t.pageNameArr.remove(i);
                    t.minMaxArr.remove(i);
                    save(t , directory + "/tableInfo.class");
                }
                else
                {
                    System.out.println("Cannot delete file");
                }
            }
        }
        t = null;
    }

    public static boolean hashSub(Hashtable<String, Object> h1, Hashtable<String, Object> h2)
    {
        boolean flag = true;
        for(int i = 0; i <= h1.size() - 1 ;i++)
        {
            String key = (String) h1.keySet().toArray()[h1.size() - i - 1];
            if(h2.containsKey(key))
            {
                if(h1.get(key) instanceof Polygon)                                                 ////////////////////////
                {
                    if(!polygonEquals((Polygon)h1.get(key),(Polygon)h2.get(key)))
                    {
                        flag = false;
                        break;
                    }
                }                                                                                 ///////////////////////
                else
                {
                    if(compare(h1.get(key),h2.get(key)) != 0)
                    {
                        flag = false;
                        break;
                    }
                }
            }
        }
        return flag;
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException, IOException, ClassNotFoundException {
        if (arrSQLTerms.length == 0)
            throw new DBAppException("No terms entered.");
        if ((arrSQLTerms.length != 1) && (strarrOperators.length != (arrSQLTerms.length-1)))
            throw new DBAppException("Operators must be less than terms by one.");
        for (SQLTerm term : arrSQLTerms) {
            if(!tableExist(term._strTableName))
                throw new DBAppException("Table does not exist.");
            if (!termContentCorrect(term))
                throw new DBAppException("Term content not correct.");
        }
        Table t = loadTableInfo("data/"+arrSQLTerms[0]._strTableName+"/tableInfo.class");
        ArrayList<Hashtable<String,Object>> result;
        if (arrSQLTerms.length == 1) {
            result = getSet(t,arrSQLTerms[0]);
        }
        else {
            boolean firstTermIndexed = t.indices.contains(arrSQLTerms[0]._strColumnName);
            boolean secondTermIndexed = t.indices.contains(arrSQLTerms[1]._strColumnName);
            ArrayList<String> pages;
            ArrayList<Hashtable<String,Object>> set;
            switch (strarrOperators[0]) {
                case "AND":
                    if (firstTermIndexed && secondTermIndexed)
                        pages = BPTree.getIntersectionPages(getPagesForTerm(arrSQLTerms[0]),getPagesForTerm(arrSQLTerms[1]));
                    else if (firstTermIndexed)
                        pages = getPagesForTerm(arrSQLTerms[0]);
                    else if (secondTermIndexed)
                        pages = getPagesForTerm(arrSQLTerms[1]);
                    else
                        pages = t.pageNameArr;
                    set = termsOP(pages,arrSQLTerms[0],arrSQLTerms[1],"AND");
                    if (arrSQLTerms.length > 2)
                        result = selectHelper(set,arrSQLTerms,strarrOperators,2);
                    else
                        result = set;
                    break;
                case "OR":
                    if (firstTermIndexed && secondTermIndexed)
                        pages = BPTree.getUnionPages(getPagesForTerm(arrSQLTerms[0]),getPagesForTerm(arrSQLTerms[1]));
                    else
                        pages = t.pageNameArr;
                    set = termsOP(pages,arrSQLTerms[0],arrSQLTerms[1],"OR");
                    if (arrSQLTerms.length > 2)
                        result = selectHelper(set,arrSQLTerms,strarrOperators,2);
                    else
                        result = set;
                    break;
                case "XOR":
                    if (firstTermIndexed && secondTermIndexed)
                        pages = BPTree.getUnionPages(getPagesForTerm(arrSQLTerms[0]),getPagesForTerm(arrSQLTerms[1]));
                    else
                        pages = t.pageNameArr;
                    set = termsOP(pages,arrSQLTerms[0],arrSQLTerms[1],"XOR");
                    if (arrSQLTerms.length > 2)
                        result = selectHelper(set,arrSQLTerms,strarrOperators,2);
                    else
                        result = set;
                    break;
                default:
                    throw new DBAppException("Operator not supported.");
            }
        }
        return result.iterator();
    }

    public static ArrayList<Hashtable<String,Object>> selectHelper(ArrayList<Hashtable<String,Object>> set, SQLTerm[] arrSQLTerms, String[] strarrOperators, int termIndex) throws DBAppException, IOException, ClassNotFoundException {
        if (termIndex == arrSQLTerms.length)
            return set;
        Table t;
        switch (strarrOperators[termIndex-1]) {
            case "AND":
                return selectHelper(filter(set,arrSQLTerms[termIndex]),arrSQLTerms,strarrOperators,termIndex+1);
            case "OR":
                t = loadTableInfo("data/"+arrSQLTerms[0]._strTableName+"/tableInfo.class");
                return selectHelper(unionSets(set,getSet(t,arrSQLTerms[termIndex])),arrSQLTerms,strarrOperators,termIndex+1);
            case "XOR":
                t = loadTableInfo("data/"+arrSQLTerms[0]._strTableName+"/tableInfo.class");
                return selectHelper(XORSets(set,getSet(t,arrSQLTerms[termIndex])),arrSQLTerms,strarrOperators,termIndex+1);
            default:
                throw new DBAppException("Operator not supported.");
        }
    }

    public static ArrayList<Hashtable<String,Object>> getSet(Table t, SQLTerm term) throws DBAppException, IOException, ClassNotFoundException {
        ArrayList<Hashtable<String,Object>> set = new ArrayList<>();
        ArrayList<String> pages;
        if (t.indices.contains(term._strColumnName)) {
            pages = getPagesForTerm(term);
        }
        else {
            pages = t.pageNameArr;
        }
        for (String page : pages) {
            Page p = loadPage("data/"+term._strTableName+ "/" +page);
            for (Hashtable<String,Object> row : p.rows) {
                switch (term._strOperator) {
                    case ">":
                        if (compare(row.get(term._strColumnName),term._objValue) > 0)
                            set.add(row);
                        break;
                    case ">=":
                        if (term._objValue instanceof Polygon) {
                            if (compare(row.get(term._strColumnName),term._objValue) > 0 || polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                                set.add(row);
                        }
                        else {
                            if (compare(row.get(term._strColumnName),term._objValue) >= 0)
                                set.add(row);
                        }
                        break;
                    case "<":
                        if (compare(row.get(term._strColumnName),term._objValue) < 0)
                            set.add(row);
                        break;
                    case "<=":
                        if (term._objValue instanceof Polygon) {
                            if (compare(row.get(term._strColumnName),term._objValue) < 0 || polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                                set.add(row);
                        }
                        else {
                            if (compare(row.get(term._strColumnName),term._objValue) <= 0)
                                set.add(row);
                        }
                        break;
                    case "!=":
                        if (term._objValue instanceof Polygon) {
                            if (!polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                                set.add(row);
                        }
                        else {
                            if (compare(row.get(term._strColumnName),term._objValue) != 0)
                                set.add(row);
                        }
                        break;
                    case "=":
                        if (term._objValue instanceof Polygon) {
                            if (polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                                set.add(row);
                        }
                        else {
                            if (compare(row.get(term._strColumnName),term._objValue) == 0)
                                set.add(row);
                        }
                        break;
                    default:
                        throw new DBAppException("Operator not supported.");
                }
            }
        }
        return set;
    }

    public static ArrayList<Hashtable<String,Object>> intersectSets(ArrayList<Hashtable<String,Object>> set1, ArrayList<Hashtable<String,Object>> set2) {
        ArrayList<Hashtable<String,Object>> result = new ArrayList<>();
        for (Hashtable<String,Object> row : set1) {
            if (set2.contains(row))
                result.add(row);
        }
        return result;
    }

    public static ArrayList<Hashtable<String,Object>> unionSets(ArrayList<Hashtable<String,Object>> set1, ArrayList<Hashtable<String,Object>> set2) {
        ArrayList<Hashtable<String, Object>> result = new ArrayList<>(set1);
        for (Hashtable<String,Object> row : set2) {
            if (!result.contains(row))
                result.add(row);
        }
        return result;
    }

    public static ArrayList<Hashtable<String,Object>> XORSets(ArrayList<Hashtable<String,Object>> set1, ArrayList<Hashtable<String,Object>> set2) {
        ArrayList<Hashtable<String,Object>> union = unionSets(set1,set2);
        ArrayList<Hashtable<String,Object>> intersect = intersectSets(set1,set2);
        union.removeAll(intersect);
        return union;
    }

    public static ArrayList<Hashtable<String,Object>> filter(ArrayList<Hashtable<String,Object>> set, SQLTerm term) throws DBAppException {
        ArrayList<Hashtable<String,Object>> result = new ArrayList<>();
        for (Hashtable<String,Object> row : set) {
            switch (term._strOperator) {
                case ">":
                    if (compare(row.get(term._strColumnName),term._objValue) > 0)
                        result.add(row);
                    break;
                case ">=":
                    if (term._objValue instanceof Polygon) {
                        if (compare(row.get(term._strColumnName),term._objValue) > 0 || polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                            result.add(row);
                    }
                    else {
                        if (compare(row.get(term._strColumnName),term._objValue) >= 0)
                            result.add(row);
                    }
                    break;
                case "<":
                    if (compare(row.get(term._strColumnName),term._objValue) < 0)
                        result.add(row);
                    break;
                case "<=":
                    if (term._objValue instanceof Polygon) {
                        if (compare(row.get(term._strColumnName),term._objValue) < 0 || polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                            result.add(row);
                    }
                    else {
                        if (compare(row.get(term._strColumnName),term._objValue) <= 0)
                            result.add(row);
                    }
                    break;
                case "!=":
                    if (term._objValue instanceof Polygon) {
                        if (!polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                            result.add(row);
                    }
                    else {
                        if (compare(row.get(term._strColumnName),term._objValue) != 0)
                            result.add(row);
                    }
                    break;
                case "=":
                    if (term._objValue instanceof Polygon) {
                        if (polygonEquals((Polygon) row.get(term._strColumnName),(Polygon) term._objValue))
                            result.add(row);
                    }
                    else {
                        if (compare(row.get(term._strColumnName),term._objValue) == 0)
                            result.add(row);
                    }
                    break;
                default:
                    throw new DBAppException("Operator not supported.");
            }
        }
        return result;
    }

    public static ArrayList<String> getPagesForTerm(SQLTerm sqlTerm) throws DBAppException, IOException, ClassNotFoundException {
        ArrayList<String> pages;
        if (sqlTerm._objValue instanceof Polygon) {
            switch (sqlTerm._strOperator) {
                case ">":
                    pages = RTree.getGreaterThan(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case ">=":
                    pages = RTree.getGreaterThanEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "<":
                    pages = RTree.getLessThan(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "<=":
                    pages = RTree.getLessThanEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "!=":
                    pages = RTree.getNotEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "=":
                    pages = RTree.getEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                default:
                    throw new DBAppException("Operator not supported.");
            }
        }
        else {
            switch (sqlTerm._strOperator) {
                case ">":
                    pages = BPTree.getGreaterThan(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case ">=":
                    pages = BPTree.getGreaterThanEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "<":
                    pages = BPTree.getLessThan(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "<=":
                    pages = BPTree.getLessThanEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "!=":
                    pages = BPTree.getNotEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                case "=":
                    pages = BPTree.getEqual(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._objValue);
                    break;
                default:
                    throw new DBAppException("Operator not supported.");
            }
        }
        return pages;
    }

    public static ArrayList<Hashtable<String,Object>> termsOP(ArrayList<String> pages, SQLTerm term1, SQLTerm term2, String op) throws DBAppException {
        ArrayList<Hashtable<String,Object>> result = new ArrayList<>();
        for (String page : pages) {
            Page p = loadPage("data/"+term1._strTableName+ "/" +page);
            for (Hashtable<String,Object> row : p.rows) {
                boolean term1Satisfied = false;
                boolean term2Satisfied = false;
                switch (term1._strOperator) {
                    case ">":
                        if (compare(row.get(term1._strColumnName),term1._objValue) > 0)
                            term1Satisfied = true;
                        break;
                    case ">=":
                        if (term1._objValue instanceof Polygon) {
                            if (compare(row.get(term1._strColumnName),term1._objValue) > 0 || polygonEquals((Polygon) row.get(term1._strColumnName),(Polygon) term1._objValue))
                                term1Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term1._strColumnName),term1._objValue) >= 0)
                                term1Satisfied = true;
                        }
                        break;
                    case "<":
                        if (compare(row.get(term1._strColumnName),term1._objValue) < 0)
                            term1Satisfied = true;
                        break;
                    case "<=":
                        if (term1._objValue instanceof Polygon) {
                            if (compare(row.get(term1._strColumnName),term1._objValue) < 0 || polygonEquals((Polygon) row.get(term1._strColumnName),(Polygon) term1._objValue))
                                term1Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term1._strColumnName),term1._objValue) <= 0)
                                term1Satisfied = true;
                        }
                        break;
                    case "!=":
                        if (term1._objValue instanceof Polygon) {
                            if (!polygonEquals((Polygon) row.get(term1._strColumnName),(Polygon) term1._objValue))
                                term1Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term1._strColumnName),term1._objValue) != 0)
                                term1Satisfied = true;
                        }
                        break;
                    case "=":
                        if (term1._objValue instanceof Polygon) {
                            if (polygonEquals((Polygon) row.get(term1._strColumnName),(Polygon) term1._objValue))
                                term1Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term1._strColumnName),term1._objValue) == 0)
                                term1Satisfied = true;
                        }
                        break;
                    default:
                        throw new DBAppException("Operator not supported.");
                }
                switch (term2._strOperator) {
                    case ">":
                        if (compare(row.get(term2._strColumnName),term2._objValue) > 0)
                            term2Satisfied = true;
                        break;
                    case ">=":
                        if (term2._objValue instanceof Polygon) {
                            if (compare(row.get(term2._strColumnName),term2._objValue) > 0 || polygonEquals((Polygon) row.get(term2._strColumnName),(Polygon) term2._objValue))
                                term2Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term2._strColumnName),term2._objValue) >= 0)
                                term2Satisfied = true;
                        }
                        break;
                    case "<":
                        if (compare(row.get(term2._strColumnName),term2._objValue) < 0)
                            term2Satisfied = true;
                        break;
                    case "<=":
                        if (term2._objValue instanceof Polygon) {
                            if (compare(row.get(term2._strColumnName),term2._objValue) < 0 || polygonEquals((Polygon) row.get(term2._strColumnName),(Polygon) term2._objValue))
                                term2Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term2._strColumnName),term2._objValue) <= 0)
                                term2Satisfied = true;
                        }
                        break;
                    case "!=":
                        if (term2._objValue instanceof Polygon) {
                            if (!polygonEquals((Polygon) row.get(term2._strColumnName),(Polygon) term2._objValue))
                                term2Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term2._strColumnName),term2._objValue) != 0)
                                term2Satisfied = true;
                        }
                        break;
                    case "=":
                        if (term2._objValue instanceof Polygon) {
                            if (polygonEquals((Polygon) row.get(term2._strColumnName),(Polygon) term2._objValue))
                                term2Satisfied = true;
                        }
                        else {
                            if (compare(row.get(term2._strColumnName),term2._objValue) == 0)
                                term2Satisfied = true;
                        }
                        break;
                    default:
                        throw new DBAppException("Operator not supported.");
                }
                switch (op) {
                    case "AND":
                        if (term1Satisfied & term2Satisfied)
                            result.add(row);
                        break;
                    case "OR":
                        if (term1Satisfied | term2Satisfied)
                            result.add(row);
                        break;
                    case "XOR":
                        if (term1Satisfied ^ term2Satisfied)
                            result.add(row);
                        break;
                    default:
                        throw new DBAppException("Operator not supported.");
                }
            }
        }
        return result;
    }

    public static void printTable(String strTableName) {
        Table table = loadTableInfo("data/" + strTableName + "/tableInfo.class");
        int count = 1;
        for(String pageName: table.pageNameArr)
        {
            Page page = loadPage("data/" + strTableName + "/" + pageName);
            System.out.println("<< Page " + count + " >>");
            count++;
            for(int i=0; i<page.rows.size(); i++)
            {
                System.out.println(page.rows.get(i));
            }
            page = null;
        }
        table = null;
    }

    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {

    }
}
