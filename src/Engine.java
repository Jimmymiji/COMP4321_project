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
    public ArrayList<Map<String,String>> retrieve(String query, Integer topK){

		ArrayList<Map<String,String>> results = new ArrayList<Map<String,String>>();
        try{
			HashMap<Integer, Double> partialScore = indexer.searchAndScore(query);
            System.out.println(partialScore);

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
			for (Map.Entry<Integer, Double> obj : list) { 
				sortedPages.add(obj.getKey()); 
			} 
            System.out.println(sortedPages);
			for(int i = 0; i < topK; i++) {
				// TODO: return details of top pages
				int pageid = sortedPages[i];
				// score, title, url, date, pagesize, key_freq (String), parents, children (ArrayList<String>)
				continue;
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

