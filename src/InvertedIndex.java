package comp4321;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;
import java.lang.Math;


public class InvertedIndex
{
    public RocksDB contentDb;
    public RocksDB titleKeyWordDb;
    public RocksDB dateDb;
    public RocksDB wordCountDb;
    public RocksDB pageSizeDb;
    public RocksDB titleDb;
    public RocksDB termWeightDb;
    public RocksDB docNormDb;
    public StopStem stopStem;
    private Options options;
    private HashMap<String, HashMap<Integer, ArrayList<Integer>>> contentInvertedTable;
    private HashMap<String, HashMap<Integer, ArrayList<Integer>>> titleInvertedTable;
    private HashMap<String, HashMap<Integer, Double>> termWeightTable;
	private HashMap<Integer,String> IDtoURLTable;
    private HashMap<Integer, Double> docNormTable;

    InvertedIndex(String ContentDbPath,String titleKeyWordDbPath,String dateDbPath,String wordCountDbPath,String pageSizeDbPath,String titleDbPath, String termWeightDbPath, String docNormDbPath) throws RocksDBException
    {
        if(!checkDbPath(ContentDbPath) ||
            !checkDbPath(titleKeyWordDbPath)  ||
            !checkDbPath(dateDbPath) ||
            !checkDbPath(wordCountDbPath) ||
            !checkDbPath(pageSizeDbPath) ||
            !checkDbPath(titleDbPath) ||
            !checkDbPath(termWeightDbPath) ||
            !checkDbPath(docNormDbPath) ){
                System.out.println("indexer check path failed");
        }
       
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.contentDb = RocksDB.open(this.options, ContentDbPath);
        this.titleKeyWordDb = RocksDB.open(this.options, titleKeyWordDbPath);
        this.dateDb = RocksDB.open(this.options,dateDbPath);
        this.wordCountDb = RocksDB.open(this.options,wordCountDbPath);
        this.pageSizeDb = RocksDB.open(this.options,pageSizeDbPath);
        this.titleDb = RocksDB.open(this.options,titleDbPath);
        this.termWeightDb = RocksDB.open(this.options,termWeightDbPath);
        this.docNormDb = RocksDB.open(this.options,docNormDbPath);
        this.stopStem = new StopStem("stopwords.txt");
        this.titleInvertedTable = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        this.contentInvertedTable = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
    	this.termWeightTable = new HashMap<String, HashMap<Integer, Double>>();
    	this.docNormTable = new HashMap<Integer, Double>();
		this.IDtoURLTable = new HashMap<Integer,String>();
    }

    public void loadFromDatabase() throws RocksDBException{
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
		
		RocksDB URLdb = RocksDB.open(this.options, "db/db");
		RocksIterator iter = URLdb.newIterator();
		// build a forwarded table: ID - URL
		for(iter.seekToFirst(); iter.isValid(); iter.next()){
			String URL = new String(iter.key());
			int PageID = Integer.parseInt(new String(iter.value()));
			if (!URL.equals("total_count")){
				this.IDtoURLTable.put(PageID,URL);
			}
		}
    }


    public void setUpSearchEngine() throws RocksDBException{
		System.out.println("setting up search engine...");
        RocksIterator termWeightIter = this.termWeightDb.newIterator();
        for(termWeightIter.seekToFirst(); termWeightIter.isValid(); termWeightIter.next()) {
        
            String word = new String(termWeightIter.key());
            String wordWeight = new String(termWeightIter.value());
            String[] docAndWeightStrs = wordWeight.split(";");
            HashMap<Integer,Double> docWeightMap = new HashMap<Integer, Double>();
            for(String weightStr : docAndWeightStrs){
                int ID = Integer.parseInt(weightStr.substring(0,weightStr.indexOf(":")));
                double weight = Double.parseDouble(weightStr.substring(weightStr.indexOf(":")+1));
                docWeightMap.put(ID,weight);
            }
            this.termWeightTable.put(word,docWeightMap);
        }

        RocksIterator docNormIter = this.docNormDb.newIterator();
        for(docNormIter.seekToFirst(); docNormIter.isValid(); docNormIter.next()) {
            int pageID = Integer.parseInt(new String(docNormIter.key()));
            double norm = Double.parseDouble(new String(docNormIter.value()));
            this.docNormTable.put(pageID,norm);
        }
    }

    public boolean titleContainsPhrase(ArrayList<ArrayList<String>> phrases, Integer pageID){
		for(int i = 0; i < phrases.size(); i++)
		{
		    if (phrases.get(i).size() <= 1){
				continue;
			}
			ArrayList<String> phrase = phrases.get(i);
			if(!this.titleInvertedTable.containsKey(phrase.get(0)) || !this.titleInvertedTable.get(phrase.get(0)).containsKey(pageID)){
				return false;
			}
			ArrayList<Integer> firstwordPos = this.titleInvertedTable.get(phrase.get(0)).get(pageID);
			for(int j = 1; j < phrase.size(); j++){
				ArrayList<Integer> currentwordPos = this.titleInvertedTable.get(phrase.get(j)).get(pageID);
				ArrayList<Integer> requiredPos = new ArrayList<Integer>();
				for(int k = 0; k < firstwordPos.size(); k++) {
					// increment required pos by j
				    int oldVal = firstwordPos.get(k);
					int newVal = oldVal + j;
					requiredPos.add(newVal);
				}
				// check whether match any item
				if(Collections.disjoint(currentwordPos, requiredPos)){
					return false;
				} // return true when no match
			}
		}
        return true;
    }

    public boolean contentContainsPhrase(ArrayList<ArrayList<String>> phrases, Integer pageID){
		for(int i = 0; i < phrases.size(); i++)
		{
		    if (phrases.get(i).size() <= 1){
				continue;
			}
			ArrayList<String> phrase = phrases.get(i);
			if(!this.contentInvertedTable.containsKey(phrase.get(0)) || !this.contentInvertedTable.get(phrase.get(0)).containsKey(pageID)){
				return false;
			}
			ArrayList<Integer> firstwordPos = this.contentInvertedTable.get(phrase.get(0)).get(pageID);
			for(int j = 1; j < phrase.size(); j++){
				ArrayList<Integer> currentwordPos = this.contentInvertedTable.get(phrase.get(j)).get(pageID);
				ArrayList<Integer> requiredPos = new ArrayList<Integer>();
				for(int k = 0; k < firstwordPos.size(); k++) {
					// increment required pos by j
				    int oldVal = firstwordPos.get(k);
					int newVal = oldVal + j;
					requiredPos.add(newVal);
				}
				// check whether match any item
				if(Collections.disjoint(currentwordPos, requiredPos)){
					return false;
				} // return true when no match
			}
		}
        return true;
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
            if (!stemWord.equals("")){
                keyWords.add(stemWord);
            }
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

    public HashMap<String,Integer> getFileWordCount(int ID){
        byte[] content = null;
        try{
            content = this.wordCountDb.get(String.valueOf(ID).getBytes());
        }catch(Exception e){
			e.printStackTrace();
		}
        if(content == null){
            return null;
        }
        HashMap<String,Integer> countTable = new HashMap<String,Integer>();
        String countString = new String(content);
        String[] pairList = countString.split(";");
        for (String pair:pairList){
            String[] p = pair.split(":");
            countTable.put(p[0],Integer.parseInt(p[1]));
        }
        return countTable;
    }

    public String getURL(int ID){
        String url = this.IDtoURLTable.get(ID);
        return url;
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

	/*update termweight = tf/tfmax*idf and doc norm*/
    public void updateTermWeightAndDocNorm() throws RocksDBException{
		System.out.println("updating Term Weight DB and Document Norm DB...");
		HashMap<Integer, Double> norms = new HashMap<Integer, Double>(); // store |Di|

		// step 1: count total number of documents: N
		RocksIterator iter = this.wordCountDb.newIterator();
		int N = 0;
		for(iter.seekToFirst(); iter.isValid(); iter.next()){
			N = N + 1;
		}

		// step 2: calculate idf, tf, tfmax
		for (Map.Entry<String, HashMap<Integer, ArrayList<Integer>>> entry : this.contentInvertedTable.entrySet()) {
		    String term = entry.getKey();
			HashMap<Integer,ArrayList<Integer>> docPosMap = entry.getValue();
			int df = docPosMap.size(); //df
			double idf = Math.log((double)N/df) / Math.log(2); //idf

			String value = new String("");
			for (Map.Entry<Integer, ArrayList<Integer>> e : docPosMap.entrySet()) {
			    Integer pageID = e.getKey();
				value = value + pageID + ":";
				ArrayList<Integer> posListInt = e.getValue();
				Integer tf = posListInt.size(); //tf
				HashMap<String, Integer> WordCount = this.getFileWordCount(pageID);
				Integer tfmax = Collections.max(WordCount.values()); //tfmax
				value = value + String.format("%.3f", ((double)tf)/tfmax * idf) + ";";

				// update norm table
				double norm = norms.containsKey(pageID) ? norms.get(pageID) : 0;
				norms.put(pageID, norm + Math.pow(((double)tf)/tfmax * idf, 2));
			}
			// write to termWeightDb
			if(value.length() > 0){
				value = value.substring(0,value.length()-1);
			}
			this.termWeightDb.put(term.getBytes(),value.getBytes());
		}
		// write to docNormDb
		for (Map.Entry<Integer, Double> entry : norms.entrySet()) {
		    String ID = Integer.toString(entry.getKey());
			String norm = Double.toString(entry.getValue());
			this.docNormDb.put(ID.getBytes(),norm.getBytes());
		}
    }


	/*return final scores (after normalization if any)*/
    public HashMap<Integer, Double> searchAndScore(Vector<String> keyWords){
		HashMap<Integer, Double> results = new HashMap<Integer, Double>();
		// calculate |Q| for cosine similarity
		double normQ = Math.sqrt(keyWords.size());

		for (String word:keyWords){
			HashMap<Integer,Double> docWeightMap = this.termWeightTable.get(word);
			if (docWeightMap == null){
				// keyword not found
				continue;
			}

			for(Map.Entry<Integer, Double> entry : docWeightMap.entrySet()) {
			    int pageID = entry.getKey();
				double weight = entry.getValue();
				// update partial score table
				double score = results.containsKey(pageID) ? results.get(pageID) : 0;
				results.put(pageID, score + weight);
			}
		}
		// update results by doing normalization (cosine)
		for (Integer key : results.keySet()) {
			results.replace(key, ((double)results.get(key)) / (Math.sqrt(this.docNormTable.get(key)) * normQ));
		}
		return results;
    }

    public static void main(String[] args){
        try
        {

            RocksDB.loadLibrary();
            
            InvertedIndex indexer = new InvertedIndex("db/db1","db/db2","db/db3","db/db4","db/db5","db/db6", "db/db7", "db/db8");
			indexer.loadFromDatabase();
			// indexer.setUpSearchEngine();
			// indexer.updateTermWeightAndDocNorm();
			// indexer.printDB(indexer.docNormDb);
			// HashMap<Integer, Double> partialScore = indexer.searchAndScore("a fucking World");
            System.out.println(indexer.getURL(1));
            System.out.println(indexer.getURL(29));
            System.out.println(indexer.getURL(18));
			/**
            // a static method that loads the RocksDB C++ library.
            System.out.println("contentDb");
            System.out.println("Key : hkust" );
            byte[] content = indexer.contentDb.get(new String("hkust").getBytes());
            System.out.println("Value : " + new String(content));

            System.out.println("");
            System.out.println("titleKeyWordDb");
            System.out.println("Key : hkust");
            content = indexer.titleKeyWordDb.get(new String("hkust").getBytes());
            System.out.println("Value : " + new String(content));

            System.out.println("");
            System.out.println("dateDb");
            System.out.println("Key : 1");
            content = indexer.dateDb.get(new String("1").getBytes());
            System.out.println("Value : " + new String(content));

            System.out.println("");
            System.out.println("wordCountDb");
            System.out.println("Key : 1");
            content = indexer.wordCountDb.get(new String("1").getBytes());
            System.out.println("Value : " + new String(content));

            System.out.println("");
            System.out.println("pageSizeDb");
            System.out.println("Key : 1");
            content = indexer.pageSizeDb.get(new String("1").getBytes());
            System.out.println("Value : " + new String(content));

            System.out.println("");
            System.out.println("tileDb");
            System.out.println("Key : 1");
            content = indexer.titleDb.get(new String("1").getBytes());
            System.out.println("Value : " + new String(content));
			*/


        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}

