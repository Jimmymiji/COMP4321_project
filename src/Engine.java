package comp4321;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.*; 
import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;


public class Engine
{
	private StopStem stopStem;
	private InvertedIndex indexer;

	Engine(String ContentDbPath,String titleKeyWordDbPath,String dateDbPath,String wordCountDbPath,String pageSizeDbPath,String titleDbPath,String termWeightDbPath,String docNormDbPath) throws RocksDBException
	{
		if(!checkDbPath(ContentDbPath) ||
			!checkDbPath(titleKeyWordDbPath)  ||
			!checkDbPath(dateDbPath) ||
			!checkDbPath(wordCountDbPath) ||
			!checkDbPath(pageSizeDbPath) ||
            !checkDbPath(termWeightDbPath) ||
            !checkDbPath(docNormDbPath) ){
				System.out.println("indexer check path failed");}
		this.indexer = new InvertedIndex(ContentDbPath,titleKeyWordDbPath,dateDbPath,wordCountDbPath,pageSizeDbPath,titleDbPath,termWeightDbPath,docNormDbPath);
		// initialize indexer
		this.indexer.loadFromDatabase();
		this.indexer.setUpSearchEngine();
	}

    public boolean checkDbPath(String path){
        if(Files.notExists(Paths.get(path))){
            File f = new File(path);
            return f.mkdir();
        }
        return true;
    }

	// retrieve topK pages based on raw query
    public ArrayList<HashMap<String,String>> retrieve(String query, Integer topK){

		ArrayList<HashMap<String,String>> results = new ArrayList<HashMap<String,String>>();
        try{
			HashMap<Integer, Double> partialScore = indexer.searchAndScore(query);

			// ranking based on score
			ArrayList<Integer> sortedPages = new ArrayList<Integer>();
			List<Map.Entry<Integer, Double>> list = new LinkedList<Map.Entry<Integer, Double>>(partialScore.entrySet());
			// sort the list
			Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() { 
				public int compare(Map.Entry<Integer, Double> e1, Map.Entry<Integer, Double> e2) { 
					Double v1 = e1.getValue(); 
					Double v2 = e2.getValue(); 
					return v2.compareTo(v1); 
				}
			}); 
			for (HashMap.Entry<Integer, Double> obj : list) { 
				sortedPages.add(obj.getKey()); 
			} 
			for(int i = 0; i < topK; i++) {
				// return details of top pages
				HashMap<String, String> page = new HashMap<String, String>();
				int pageid = sortedPages.get(i);
				page.put("score", String.valueOf(partialScore.get(pageid)));
				page.put("url", this.indexer.getURL(pageid));
				page.put("title", this.indexer.getPageTitle(pageid));
				// date
				Date date =  new Date(this.indexer.getLastModifiedData(String.valueOf(pageid)));  
				DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");  
				String strDate = dateFormat.format(date);  
				page.put("date", strDate);
				page.put("pagesize", String.valueOf(this.indexer.getPageSize(pageid)));
				// key frequency
				HashMap<String, Integer> WordCount = this.indexer.getFileWordCount(pageid);
				String keyFreq = "";
				for (Map.Entry<String, Integer> entry : WordCount.entrySet()) {
				    String key = entry.getKey();
					String value = String.valueOf(entry.getValue());
					keyFreq = keyFreq + key + ":" + value + ";";
				}
				if(keyFreq.length() > 0){
					keyFreq = keyFreq.substring(0, keyFreq.length()-1);
				}
				page.put("key_freq", keyFreq);
				// parents, children
				FileLink fl = new FileLink();
				fl.readLinkDB("linkdb");
				HashSet<String> childLinks = fl.getChildren(String.valueOf(pageid));
				HashSet<String> parentLinks = fl.getParents(String.valueOf(pageid));
				String children = "";
				String parents = "";
				for (String link : parentLinks) {
				   parents = parents + this.indexer.getURL(Integer.parseInt(link)) + ";";
				}
				for (String link : childLinks) {
				   children = children + this.indexer.getURL(Integer.parseInt(link)) + ";";
				}
				if(parents.length() > 0){
					parents = parents.substring(0, parents.length()-1);
				}
				if(children.length() > 0){
					children = children.substring(0, children.length()-1);
				}
				page.put("parents", parents);
				page.put("children", children);
				results.add(page);
			}
        }catch(Exception e){
			e.printStackTrace();
		}
		if(results.size()==0){
			return null;	
		}
		return results;
    }

    public static void main(String[] args){
        try
        {
			// test program
            Engine engine = new Engine("db/db1","db/db2","db/db3","db/db4","db/db5", "db/db6", "db/db7", "db/db8");
			engine.retrieve("computer a fucking world", 5);

        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}

