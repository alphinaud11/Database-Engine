package MOG_SQL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Overflow implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;
    ArrayList<Hashtable<String,Object>> pagesAndOccurrences;



    public Overflow(){
        this.pagesAndOccurrences = new ArrayList<>();
    }

    public void addPage(String page) {  //Added by Helmy
        if (pagesAndOccurrences.size() == 0) {
            Hashtable<String,Object> h = new Hashtable<>();
            h.put("name",page);
            h.put("occurrence",1);
            pagesAndOccurrences.add(h);
        }
        else {
            if (!this.pageExists(page)) {
                Hashtable<String,Object> h = new Hashtable<>();
                h.put("name",page);
                h.put("occurrence",1);
                pagesAndOccurrences.add(h);
                for (int i=1; i<pagesAndOccurrences.size(); i++) {
                    Hashtable<String,Object> key = pagesAndOccurrences.get(i);
                    String keyPage = (String) key.get("name");
                    int pageNumI = Integer.parseInt(keyPage.substring(4,keyPage.length()-6));
                    int j = i - 1;
                    String pageJ = (String) pagesAndOccurrences.get(j).get("name");
                    int pageNumJ = Integer.parseInt(pageJ.substring(4,pageJ.length()-6));
                    while ((j >= 0) && (DBApp.compare(pageNumJ,pageNumI) > 0)) {
                        pagesAndOccurrences.add(j+1,pagesAndOccurrences.get(j));
                        pagesAndOccurrences.remove(j+2);
                        j = j - 1;
                        if (j >= 0) {
                            pageJ = (String) pagesAndOccurrences.get(j).get("name");
                            pageNumJ = Integer.parseInt(pageJ.substring(4,pageJ.length()-6));
                        }
                    }
                    pagesAndOccurrences.add(j+1,key);
                    pagesAndOccurrences.remove(j+2);
                }
            }
            else {
                for (Hashtable<String, Object> h : pagesAndOccurrences) {
                    if (page.equals(h.get("name"))) {
                        int occ = (int) h.get("occurrence");
                        h.replace("occurrence",occ+1);
                        return;
                    }
                }
            }
        }
    }

    public boolean pageExists(String page) {  //Added by Helmy
        for (Hashtable<String, Object> nameOcc : pagesAndOccurrences) {
            if (page.equals(nameOcc.get("name"))) {
                return true;
            }
        }
        return false;
    }

    public void delete(String page) {  //Added by Helmy
        for (Hashtable<String, Object> h : pagesAndOccurrences) {
            if (page.equals(h.get("name"))) {
                int occ = (int) h.get("occurrence");
                occ = occ - 1;
                if (occ != 0)
                    h.replace("occurrence",occ);
                else
                    pagesAndOccurrences.remove(h);
                return;
            }
        }
    }

}
