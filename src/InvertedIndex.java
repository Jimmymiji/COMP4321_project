package comp4321;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;


public class InvertedIndex
{
    public RocksDB contentDb;
    public RocksDB titleKeyWordDb;
    public RocksDB dateDb;
    public RocksDB wordCountDb;
    public RocksDB pageSizeDb;
    public RocksDB titleDb;
    private Options options;
    private HashMap<String, HashMap<Integer, ArrayList<Integer>>> contentInvertedTable;
    private HashMap<String, HashMap<Integer, ArrayList<Integer>>> titleInvertedTable;
    private StopStem stopStem;

    InvertedIndex(String ContentDbPath,String titleKeyWordDbPath,String dateDbPath,String wordCountDbPath,String pageSizeDbPath,String titleDbPath) throws RocksDBException
    {
        if(!checkDbPath(ContentDbPath) ||
            !checkDbPath(titleKeyWordDbPath)  ||
            !checkDbPath(dateDbPath) ||
            !checkDbPath(wordCountDbPath) ||
            !checkDbPath(pageSizeDbPath) ||
            !checkDbPath(titleDbPath) ){
                System.out.println("indexer check path failed");
        }
       
        System.out.println("init indexer");
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.contentDb = RocksDB.open(this.options, ContentDbPath);
        this.titleKeyWordDb = RocksDB.open(this.options, titleKeyWordDbPath);
        this.dateDb = RocksDB.open(this.options,dateDbPath);
        this.wordCountDb = RocksDB.open(this.options,wordCountDbPath);
        this.pageSizeDb = RocksDB.open(this.options,pageSizeDbPath);
        this.titleDb = RocksDB.open(this.options,titleDbPath);
        this.stopStem = new StopStem("stopwords.txt");
        this.titleInvertedTable = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        this.contentInvertedTable = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
    }

    public void loadFromDatabse() throws RocksDBException{
        RocksIterator contentIter = this.contentDb.newIterator();
        for(contentIter.seekToFirst(); contentIter.isValid(); contentIter.next()) {
        
            String word = new String(contentIter.key());
            String wordOccurence = new String(contentIter.value());
            String[] docAndPosStrs = wordOccurence.split(";");
            HashMap<Integer,ArrayList<Integer>> docPosMap = new HashMap<Integer, ArrayList<Integer>>();
            for(String posStr : docAndPosStrs){
                int ID = Integer.parseInt(posStr.substring(0,posStr.indexOf(":")));
                String pos = posStr.substring(posStr.indexOf(":")+1);
                String[] posList = pos.split(",");
                ArrayList<Integer> posListInt = new ArrayList<>();
                for (String p : posList){
                    posListInt.add(Integer.parseInt(p));
                }
                docPosMap.put(ID,posListInt);
            }
            this.contentInvertedTable.put(word,docPosMap);
        }

        RocksIterator titleIter = this.titleKeyWordDb.newIterator();
        for(titleIter.seekToFirst(); titleIter.isValid(); titleIter.next()) {
        
            String word = new String(titleIter.key());
            String wordOccurence = new String(titleIter.value());
            String[] docAndPosStrs = wordOccurence.split(";");
            HashMap<Integer,ArrayList<Integer>> docPosMap = new HashMap<Integer, ArrayList<Integer>>();
            for(String posStr : docAndPosStrs){
                int ID = Integer.parseInt(posStr.substring(0,posStr.indexOf(":")));
                String pos = posStr.substring(posStr.indexOf(":")+1);
                String[] posList = pos.split(",");
                ArrayList<Integer> posListInt = new ArrayList<>();
                for (String p : posList){
                    posListInt.add(Integer.parseInt(p));
                }
                docPosMap.put(ID,posListInt);
            }
            this.titleInvertedTable.put(word,docPosMap);
        }

    }


    public void writeToDatabase() throws RocksDBException{
        Iterator<HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>>> contentIt = this.contentInvertedTable.entrySet().iterator();
        while(contentIt.hasNext()){
            HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>> pair = (HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>>) contentIt.next();
            String word = pair.getKey();
            Map<Integer, ArrayList<Integer>> docPosMap = pair.getValue();
            String value = new String("");
            Iterator<HashMap.Entry<Integer,ArrayList<Integer>>> it = docPosMap.entrySet().iterator();
            while(it.hasNext()){
                HashMap.Entry<Integer,ArrayList<Integer>> idPosPair = (HashMap.Entry<Integer,ArrayList<Integer>>) it.next();
                int ID = idPosPair.getKey();
                ArrayList<Integer> posListInt = idPosPair.getValue();
                value = value + ID + ":";
                for(int p:posListInt){
                    value = value + p + ",";
                }
                value = value.substring(0,value.length()-1);
                value = value + ";";
            }
            if(value.length() > 0){
                value = value.substring(0,value.length()-1);
            }
            this.contentDb.put(word.getBytes(),value.getBytes());
        }

        Iterator<HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>>> titleIt = this.titleInvertedTable.entrySet().iterator();
        while(titleIt.hasNext()){
            HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>> pair = (HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>>) titleIt.next();
            String word = pair.getKey();
            HashMap<Integer, ArrayList<Integer>> docPosMap = pair.getValue();
            String value = new String("");
            Iterator<HashMap.Entry<Integer,ArrayList<Integer>>> it = docPosMap.entrySet().iterator();
            while(it.hasNext()){
                HashMap.Entry<Integer,ArrayList<Integer>> idPosPair = (HashMap.Entry<Integer,ArrayList<Integer>>) it.next();
                int ID = idPosPair.getKey();
                ArrayList<Integer> posListInt = idPosPair.getValue();
                value = value + ID + ":";
                for(int p:posListInt){
                    value = value + p + ",";
                }
                value = value.substring(0,value.length()-1);
                value = value + ";";
            }
            value = value.substring(0,value.length()-1);
            this.titleKeyWordDb.put(word.getBytes(),value.getBytes());
        }
    }


    public void addCountTitle(String word, int ID, int pos){
        HashMap<Integer,ArrayList<Integer>> docPosMap = this.titleInvertedTable.get(word);
        if (docPosMap == null){
            docPosMap = new HashMap<Integer,ArrayList<Integer>>();
            this.titleInvertedTable.put(word,docPosMap);
        }
        ArrayList<Integer> posListInt = docPosMap.get(ID);
        if (posListInt == null){
            posListInt = new ArrayList();
            docPosMap.put(ID,posListInt);
        }
        posListInt.add(pos);
        Collections.sort(posListInt);
    }


    public void addCountContent(String word, int ID, int pos){
        HashMap<Integer,ArrayList<Integer>> docPosMap = this.contentInvertedTable.get(word);
        if (docPosMap == null){
            docPosMap = new HashMap<Integer,ArrayList<Integer>>();
            this.contentInvertedTable.put(word,docPosMap);
        }
        ArrayList<Integer> posListInt = docPosMap.get(ID);
        if (posListInt == null){
            posListInt = new ArrayList();
            docPosMap.put(ID,posListInt);
        }
        posListInt.add(pos);
        Collections.sort(posListInt);
    }


    public void updateOnePage(Vector<String> content,Vector<String> title,int ID,String date){
        if(content.size()==0){
            content.add(new String("NULL"));
        }
        if(title.size()==0){
            title.add(new String("NULL"));
        }
        Vector<String> keyWords = new Vector<String>();
        int pageSize = 0;
        String titleString = new String();
        for(int i = 0;i<content.size();i++){
            String word = content.get(i);
            pageSize = pageSize + word.length();
            if (stopStem.isStopWord(word)){
                continue;
            }
            String stemWord = stopStem.stem(word);
            addCountContent(stemWord,ID,i);
            keyWords.add(stemWord);
        }
        for(int i = 0;i<title.size();i++){
            String word = title.get(i);
            titleString = titleString + word + " ";
            if (stopStem.isStopWord(word)){
                continue;
            }
            addCountTitle(stopStem.stem(word),ID,i);
        }
        titleString = titleString.substring(0,titleString.length()-1);
        countKeyWordInFile(keyWords,ID);
        try{
            this.dateDb.put(String.valueOf(ID).getBytes(),date.getBytes());
            this.pageSizeDb.put(String.valueOf(ID).getBytes(),String.valueOf(pageSize).getBytes());
            this.titleDb.put(String.valueOf(ID).getBytes(),titleString.getBytes());
        }
        catch(Exception e){
			e.printStackTrace();
		}
    }

    public void countKeyWordInFile(Vector<String> words,int ID){
        if(words.size()==0){
            words.add(new String("NULL"));
        }
        HashMap<String,Integer> countTable = new HashMap<String,Integer>();
        for (String word:words){
            Integer count = countTable.get(word);
            if(count == null){
                countTable.put(word,1);
            }else{
                countTable.put(word,count+1);
            }
        }
        Map<String, Integer> sorted = countTable.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(
            toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2,
            LinkedHashMap::new));
        String countString = new String();
        for (Map.Entry<String,Integer> entry : sorted.entrySet()) {
            countString = countString + entry.getKey() + ":" + entry.getValue() + ";";
        }
        countString = countString.substring(0,countString.length()-1);
        try{
            this.wordCountDb.put(String.valueOf(ID).getBytes(),countString.getBytes());
        }
        catch(Exception e){
			e.printStackTrace();
		}
        
    }

    public HashMap<String,String> getFileWordCount(int ID){
        byte[] content = null;
        try{
            content = this.wordCountDb.get(String.valueOf(ID).getBytes());
        }catch(Exception e){
			e.printStackTrace();
		}
        if(content == null){
            return null;
        }
        HashMap<String,String> countTable = new HashMap<String,String>();
        String countString = new String(content);
        String[] pairList = countString.split(";");
        for (String pair:pairList){
            String[] p = pair.split(":");
            countTable.put(p[0],p[1]);
        }
        return countTable;
    }

    public Long getLastModifiedData(String ID) throws RocksDBException { 
        byte[] content = this.dateDb.get(ID.getBytes());
        if (content == null){
            return new Long(0);
        }else{
            String dateString = new String(content);
            return Long.valueOf(dateString);
        }
    }

    public int getPageSize(int ID) throws RocksDBException{
        byte[] content = this.pageSizeDb.get(String.valueOf(ID).getBytes());
        if (content == null){
            return 0;
        }else{
            return Integer.valueOf(new String(content));
        }
    }

    public String getPageTitle(int ID) throws RocksDBException {
        byte[] content = this.titleDb.get(String.valueOf(ID).getBytes());
        if (content == null){
            return null;
        }
        else{
            return new String(content);
        }
    }

	public void printDB(RocksDB db){
        RocksIterator iter = db.newIterator();

        for(iter.seekToFirst(); iter.isValid(); iter.next())
        {
            System.out.println("key: " + new String(iter.key()) + ", value: " + new String(iter.value()));
        }
    }

    public boolean checkDbPath(String path){
        if(Files.notExists(Paths.get(path))){
            File f = new File(path);
            return f.mkdir();
        }
        return true;
    }

    public static void main(String[] args){
        try
        {
            // a static method that loads the RocksDB C++ library.
            RocksDB.loadLibrary();
            
            InvertedIndex indexer = new InvertedIndex("db/db1","db/db2","db/db3","db/db4","db/db5","db/db6");
            indexer.loadFromDatabse();
            indexer.printDB(indexer.titleDb);
            System.out.println("title of 10: "+indexer.getPageTitle(10));
            System.out.println("title of 20: "+indexer.getPageTitle(20));

        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}

