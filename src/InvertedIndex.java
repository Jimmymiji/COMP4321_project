package comp4321;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;
import java.util.*;



public class InvertedIndex
{
    public RocksDB contentDb;
    public RocksDB titleDb;
    public RocksDB dateDb;
    private Options options;
    private HashMap<String, HashMap<Integer, ArrayList<Integer>>> contentInvertedTable;
    private HashMap<String, HashMap<Integer, ArrayList<Integer>>> titleInvertedTable;
    private StopStem stopStem;

    InvertedIndex(String ContentDbPath,String TitleDbPath,String dateDbPath) throws RocksDBException
    {
        System.out.println("init indexer");
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.contentDb = RocksDB.open(this.options, ContentDbPath);
        this.titleDb = RocksDB.open(this.options, TitleDbPath);
        this.dateDb = RocksDB.open(this.options,dateDbPath);
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

        RocksIterator titleIter = this.titleDb.newIterator();
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
            value = value.substring(0,value.length()-1);
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
            this.titleDb.put(word.getBytes(),value.getBytes());
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
        for(int i = 0;i<content.size();i++){
            String word = content.get(i);
            if (stopStem.isStopWord(word)){
                continue;
            }
            addCountContent(stopStem.stem(word),ID,i);
        }
        for(int i = 0;i<title.size();i++){
            String word = title.get(i);
            if (stopStem.isStopWord(word)){
                continue;
            }
            addCountTitle(stopStem.stem(word),ID,i);
        }
        try{
            this.dateDb.put(String.valueOf(ID).getBytes(),date.getBytes());
        }
        catch(Exception e){
			e.printStackTrace();
		}
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

	public void printDB(RocksDB db){
        RocksIterator iter = db.newIterator();

        for(iter.seekToFirst(); iter.isValid(); iter.next())
        {
            System.out.println("key: " + new String(iter.key()) + ", value: " + new String(iter.value()));
        }
    }

    public static void main(String[] args){
        try
        {
            // a static method that loads the RocksDB C++ library.
            RocksDB.loadLibrary();
            InvertedIndex indexer = new InvertedIndex("db/db1","db/db2","db/db3");
            indexer.loadFromDatabse();

        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}

