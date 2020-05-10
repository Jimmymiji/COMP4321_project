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
import java.util.StringTokenizer;
import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;



public class Engine
{
	public StopStem stopStem;
	public InvertedIndex indexer;
	private String linkDbPath;

	public Engine(String ContentDbPath,String titleKeyWordDbPath,String dateDbPath,String wordCountDbPath,String pageSizeDbPath,String titleDbPath,String termWeightDbPath,String docNormDbPath,String urlDbPath,String linkDbPath) throws RocksDBException
	{
		if(!checkDbPath(ContentDbPath) ||
			!checkDbPath(titleKeyWordDbPath)  ||
			!checkDbPath(dateDbPath) ||
			!checkDbPath(wordCountDbPath) ||
			!checkDbPath(titleDbPath) ||
			!checkDbPath(pageSizeDbPath) ||
            !checkDbPath(termWeightDbPath) ||
            !checkDbPath(docNormDbPath) ||
			!checkDbPath(urlDbPath)) {
				System.out.println("indexer check path failed");}
		this.linkDbPath = linkDbPath;
		this.indexer = new InvertedIndex(ContentDbPath,titleKeyWordDbPath,dateDbPath,wordCountDbPath,pageSizeDbPath,titleDbPath,termWeightDbPath,docNormDbPath,urlDbPath);
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

    public ArrayList<Integer> reorderOnPhrase(ArrayList<ArrayList<String>> phrases, ArrayList<Integer> sortedPages){
		// mechanism: return matched page first, then no matched pages based on score
		ArrayList<Integer> newSortedPages = new ArrayList<Integer>();
		ArrayList<Integer> noMatchPhrasePages = new ArrayList<Integer>();
		for(int i=0; i < sortedPages.size(); i++){
			int pageid = sortedPages.get(i);
			if(this.indexer.contentContainsPhrase(phrases, pageid) || this.indexer.titleContainsPhrase(phrases, pageid)){
				newSortedPages.add(pageid);
			} else {
				noMatchPhrasePages.add(pageid);	
			}
		}
		newSortedPages.addAll(noMatchPhrasePages);
		return newSortedPages;
    }

	/*retrieve topK pages based on raw query*/
    public ArrayList<HashMap<String,String>> retrieve(String query, Integer topK){

		ArrayList<HashMap<String,String>> results = new ArrayList<HashMap<String,String>>();
		Vector<String> queryVector = new Vector<String>();
		StringTokenizer st = new StringTokenizer(query);
		while (st.hasMoreTokens()) {
			queryVector.add(st.nextToken());
		}
		// stop and stem for raw query
		Vector<String> keyWords = new Vector<String>();
		ArrayList<ArrayList<String>> phrases = new ArrayList<ArrayList<String>>();
		int flag = 0; 
		ArrayList<String> phrase = new ArrayList<String>();
		for(int i = 0;i < queryVector.size(); i++){
			String word = queryVector.get(i);
			if (this.indexer.stopStem.isStopWord(word)){
				continue;
			}
			if (word.startsWith("\"") || (flag==1 && !word.endsWith("\""))){
				flag = 1; // put this word in a phrase
			} else if (flag == 1 && word.endsWith("\"")){
				flag = 2; // this is the final word of a phrase
			} else {
				flag = 0;
			}
			String stemWord = this.indexer.stopStem.stem(word);
            if (!stemWord.equals("")){
				keyWords.add(stemWord);
				phrase.add(stemWord);
				if (flag == 2){
					ArrayList<String> clone = new ArrayList<String>(); 
					clone.addAll(phrase);
					phrases.add(clone);
					phrase.clear();
				} else if (flag == 0){
					ArrayList<String> clone = new ArrayList<String>(); 
					clone.addAll(phrase);
					phrases.add(clone);
					phrase.clear();
				}
			}
		}
        try{
			HashMap<Integer, Double> partialScore = indexer.searchAndScore(keyWords);

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
			sortedPages = reorderOnPhrase(phrases, sortedPages);
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
					keyFreq = keyFreq + key + ": " + value + "; ";
				}
				if(keyFreq.length() > 0){
					keyFreq = keyFreq.substring(0, keyFreq.length()-1);
				}
				page.put("key_freq", keyFreq);
				// parents, children
				FileLink fl = new FileLink();
				fl.readLinkDB(this.linkDbPath);
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
			HashMap<String, String> page = new HashMap<String,String>();
			page.put(e.toString(),e.toString());
			results.add(page);
		}
		// if(results.size()==0){
		// 	return null;	
		// }
		return results;
    }

    public static void main(String[] args){
        try
        {
			// test program
            Engine engine = new Engine("db/db1","db/db2","db/db3","db/db4","db/db5","db/db6","db/db7","db/db8","db/db","linkdb");
			ArrayList<HashMap<String,String>> results = engine.retrieve(args[0], 5);
			/*
			for(HashMap<String,String> p:results){
				System.out.println("New page:-----------------------------");
				for(Map.Entry<String,String> e: p.entrySet()){
					System.out.println(e.getKey());
					System.out.println(e.getValue());
				}
			}
			*/
        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}

// public Engine createE(String ContentDbPath,String titleKeyWordDbPath,String dateDbPath,String wordCountDbPath,String pageSizeDbPath,String titleDbPath,String termWeightDbPath,String docNormDbPath,String urlDbPath)
// 	{
// 		Engine e;
// 		try{
// 			e = new Engine(ContentDbPath,titleKeyWordDbPath,dateDbPath,wordCountDbPath,pageSizeDbPath,titleDbPath,termWeightDbPath,docNormDbPath,urlDbPath);
// 		}
// 		catch(RocksDBException e)
//     	{
//         	System.err.println(e.toString());
//     	}
// 		return e;
// 	}
