package comp4321;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.util.LinkedList;
import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Date;
import org.htmlparser.beans.StringBean;
import org.htmlparser.beans.FilterBean;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import java.util.StringTokenizer;
import org.htmlparser.beans.LinkBean;
import java.net.HttpURLConnection;
import org.htmlparser.http.HttpHeader;
import java.net.URL;
import java.io.*;


public class Spider
{
	private String url;
	public Spider(String _url)
	{
		url = _url;
	}
	public Vector<String> extractWords(String url) throws ParserException
	{
		// extract words in url and return them
		// use StringTokenizer to tokenize the result from StringBean
		// ADD YOUR CODES HERE
		Vector<String> result = new Vector<String>();
		StringBean bean = new StringBean();
        bean.setURL(url);
        bean.setLinks(false);
        String contents = bean.getStrings();
        StringTokenizer st = new StringTokenizer(contents);
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return result;
			
	}
	public Vector<String> extractLinks(String url) throws ParserException

	{
		// extract links in url and return them
		// ADD YOUR CODES HERE
		Vector<String> result = new Vector<String>();
        LinkBean bean = new LinkBean();
        bean.setURL(url);
        URL[] urls = bean.getLinks();
        for (URL s : urls) {
            result.add(s.toString());
        }
        return result;
                
	}

	public Vector<String> extractTitle(String url) throws ParserException
	{
		Vector<String> result = new Vector<String>();
		FilterBean fb = new FilterBean ();
     	fb.setFilters (new NodeFilter[] { new TagNameFilter ("title") });
     	fb.setURL (url);
        StringTokenizer st = new StringTokenizer(fb.getText());
            while (st.hasMoreTokens()) {
                result.add(st.nextToken());
            }
            return result;
	}

	// Crawl a single webpage. Return a vector with size > 0 if the webpage has not
	// been crawled before.
	public Vector<Vector<String>> crawlWebpage(RocksDB db, Vector<String> vec,FileLink fl,InvertedIndex indexer){
		Vector<Vector<String>> res = new Vector<Vector<String>>();

		try{
			String url = vec.get(1);
			URL obj = new URL(url);
			HttpURLConnection conn = (HttpURLConnection) obj.openConnection();
			long date = conn.getLastModified();
			long prev_date = date;
			byte[] content = db.get(url.getBytes());

			if (content == null){
				prev_date = new Long(0);
			}else{
				String id_string = new String(content);
				prev_date = indexer.getLastModifiedData(id_string);
			} 
			if(content!=null){
				if(date>0 && date<=prev_date){   
					fl.updateLinkDB(db,vec);
					return res;
				}
			}
			Vector<String> words = extractWords(url);
			Vector<String> links = extractLinks(url);
			Vector<String> title = extractTitle(url);
			Vector<String> date_vec = new Vector<String>(); // store date string as vector to return it.
			date_vec.add(String.valueOf(date));

			res.add(words);
			res.add(links);
			res.add(date_vec);
			res.add(title);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return res;
	}
	
	//crawl webpages in a BFS way
	public void BFSCrawl(RocksDB db, int page_count, FileLink fl,InvertedIndex indexer){
		try{
			int count = Integer.parseInt(new String(db.get("total_count".getBytes())));
			LinkedList<Vector<String>> q = new LinkedList<Vector<String>>();
			Vector<String> start_vec = new Vector<String>();
			start_vec.add(null);
			start_vec.add(url);
			q.add(start_vec);

			while(count<page_count && q.size()>0){
				Vector<String> vec = q.remove();
				Vector<Vector<String>> crawl_res = crawlWebpage(db,vec,fl,indexer); 

				if(crawl_res.size()>0){// a new page has been crawled!
					System.out.println("Crawling "+vec.get(1));
					Vector<String> links = crawl_res.get(1);

					for(int i=0;i<links.size();i++){
						Vector<String> page_vec = new Vector<String>();
						page_vec.add(vec.get(1));
						page_vec.add(links.get(i));
						q.add(page_vec);
					}

					if(db.get(vec.get(1).getBytes()) == null){
						count = addUrl(db, vec.get(1));
					}
					

					Vector<String> id_date = new Vector<String>(); //ID and date
					id_date.add(String.valueOf(count));
					id_date.add(crawl_res.get(2).get(0));

					Vector<Vector<String>> cnt_link = new Vector<Vector<String>>();//content and links
					cnt_link.add(crawl_res.get(0));
					cnt_link.add(crawl_res.get(1));

					// indexer function here: update db 
					// 1. last modified date and 
					// 2. inverted file for contents
					// params: 
					//	id_date: doc id and last modified date
					//	cnt_link: content and link of the web page
					indexer.updateOnePage(crawl_res.get(0),crawl_res.get(3),count,crawl_res.get(2).get(0));

					fl.updateLinkDB(db,vec);
				}
			}
			indexer.writeToDatabase();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// update the URL--ID database
	public int addUrl(RocksDB db, String url){
		try{
			int count = Integer.parseInt(new String(db.get("total_count".getBytes())));
			count += 1;
			db.remove("total_count".getBytes());
			db.put("total_count".getBytes(),String.valueOf(count).getBytes());
			db.put(url.getBytes(),String.valueOf(count).getBytes());
			return count;
		}
		catch(RocksDBException e){
			e.printStackTrace();
		}
		return -1;
	}

	// store contents of webpages, not used.
	public void storeFile(String filename, Vector<String> words, Vector<String> links){

		try {
			File dir = new File("./docs");
			if(!dir.exists() || !dir.isDirectory()){//Creating the directory
				dir.mkdirs();
			}
			File doc_file= new File("./docs/",filename);
			FileWriter writer = new FileWriter(doc_file);

			writer.write("Content:\n");
      		for(int i = 0; i < words.size(); i++)
				writer.write(words.get(i)+" ");
			writer.write("\n");

			writer.write("Link:\n");
			for(int i = 0; i < links.size(); i++)		
				writer.write(links.get(i)+"\n");
			writer.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}

	}

	public void printdb(RocksDB db){
        RocksIterator iter = db.newIterator();

        for(iter.seekToFirst(); iter.isValid(); iter.next())
        {
            // Get and print the content of each key
            System.out.println("key: " + new String(iter.key()) + ", value: " + new String(iter.value()));
        }
		
	}
	
	public static void main (String[] args)
	{

		String dbPath = "db/db";
		InvertedIndex indexer;
		try{
			indexer = new InvertedIndex("db/db1","db/db2","db/db3","db/db4","db/db5","db/db6", "db/db7", "db/db8","db/db");
			indexer.loadFromDatabase();
		}
		catch (RocksDBException rdbe) {
        	rdbe.printStackTrace(System.err);
			return ;
       	}
		System.out.println("indexer in spider init done");
       // A static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

		try
		{
            Options options = new Options();
            options.setCreateIfMissing(true);

            //Creat and open the database;
            RocksDB db = indexer.URLdb; 
            //db = RocksDB.open(options, dbPath);

			// if (db.get("total_count".getBytes()) == null){
			// 	db.put("total_count".getBytes(), "0".getBytes());
			// }

			
			FileLink fl = new FileLink();
			fl.readLinkDB("linkdb");
			//fl.printLinkDB();
			System.out.println("init Spider");
			// javac -cp lib/htmlparser.jar:lib/rocksdbjni-6.8.0-linux64.jar COMP4321/*.java 
			// java -cp lib/rocksdbjni-6.8.0-linux64.jar:lib/htmlparser.jar:. COMP4321.Spider
			Spider spider = new Spider("https://cse.ust.hk");
			//"https://stackoverflow.com/questions/7846136/how-to-get-last-modified-date-from-header-to-string"
			spider.BFSCrawl(db,100, fl,indexer);
			spider.printdb(db);
			fl.storeLinkDB("linkdb");
			indexer.updateTermWeightAndDocNorm();
			
		}
		catch (Exception e)
        {
            e.printStackTrace ();
        }

	}
}
	
