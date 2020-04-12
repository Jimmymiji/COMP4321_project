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


public class Test
{

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
            InvertedIndex indexer = new InvertedIndex("db/db1","db/db2","db/db3","db/db4","db/db5", "db/db6");
            indexer.loadFromDatabse();
			// URL-ID DB
			Options options = new Options();
			options.setCreateIfMissing(true);
			RocksDB URLdb = RocksDB.open(options, "db/db");
			RocksIterator iter = URLdb.newIterator();
			// build a forwarded table: ID - URL
			HashMap<String,String> IDtoURL = new HashMap<String,String>();
			// indexer.printDB(URLdb);
			for(iter.seekToFirst(); iter.isValid(); iter.next())
			{
				String URL = new String(iter.key());
				String PageID = new String(iter.value());
				if (!URL.equals("total_count")){
					IDtoURL.put(PageID,URL);
				}
			}

			for(iter.seekToFirst(); iter.isValid(); iter.next())
			{
				String URL = new String(iter.key());
				int PageID = Integer.parseInt(new String(iter.value()));

				if (URL == null || URL.equals("total_count")) {
					continue;
				}
				// 1. Page title
				System.out.println(indexer.getPageTitle(PageID));

				// 2. URL
				System.out.println(URL);

				// 3. Last modification date, size of page
				System.out.println(new Date(indexer.getLastModifiedData(String.valueOf(PageID))) + ", " + String.valueOf(indexer.getPageSize(PageID)));

				// 4. Keyword - Frequency
				System.out.println("Key-Frequency: ");
				HashMap<String, String> WordCount = indexer.getFileWordCount(PageID);
				WordCount.forEach((key,value) -> System.out.printf(key + " " + value + "; "));
				System.out.println("");

				// 5. Child links
				System.out.println("Child Links: ");
				FileLink fl = new FileLink();
				fl.readLinkDB("linkdb");
				HashSet<String> ChildLinks = fl.getChildren(String.valueOf(PageID));
				for (String link : ChildLinks) {
				   System.out.println(IDtoURL.get(link));
			    }
				
				// 6. Separation line
				System.out.println("--------------------------------------");

			}
			// indexer.printDB(indexer.dateDb);

        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}

