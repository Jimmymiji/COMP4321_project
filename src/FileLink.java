package comp4321;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.util.HashSet;
import java.util.Vector;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.io.*;


public class FileLink
{
	private HashMap<String, Links> linkDB;

	FileLink(){
 		linkDB = new HashMap<String, Links>();
	}

	public HashMap<String, Links> getLinkDB(){
		return linkDB;
	}

	public void readLinkDB(String filename){
      	try {
      		File f = new File(filename);
			if(f.exists() && f.isFile()){
	        	FileInputStream fileIn = new FileInputStream(f);
	        	ObjectInputStream in = new ObjectInputStream(fileIn);
	        	linkDB = (HashMap<String, Links>) in.readObject();
	        	in.close();
	        	fileIn.close();
        	}
      	} catch (Exception i) {
        	i.printStackTrace();
        	return;
		}
	}

	public void updateLinkDB(RocksDB db, Vector<String> vec){
		try{
			String p = vec.get(0);
			String c = vec.get(1);

			String cID = new String(db.get(c.getBytes()));
			Links link = new Links(new HashSet<String>(), new HashSet<String>());
			linkDB.put(cID, link);
			if(p==null){//the starting URL
				return;
			}
			String pID = new String(db.get(p.getBytes()));
			linkDB.get(pID).getChildren().add(cID);
			linkDB.get(cID).getParents().add(pID);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return;
	}

	public void printLinkDB(){
		Iterator it = linkDB.entrySet().iterator();
    	while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey());
	        System.out.print("Parent: ");
	        Links link = (Links) pair.getValue();
	        for(String parent : link.getParents()){
   				System.out.println(parent);
			}
			System.out.println();
			System.out.print("Child: ");
	        for(String child : link.getChildren()){
   				System.out.println(child);
			}
			System.out.println();
    	}
	}

	public void storeLinkDB(String filename){
		try {
        	FileOutputStream fileOut = new FileOutputStream(filename);
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(linkDB);
	        out.close();
	        fileOut.close();
         	System.out.println("Serialized data is saved in "+filename);
      	} catch (IOException i) {
        	i.printStackTrace();
      	}

	}

	public HashSet<String> getChildren(String ID){
		return linkDB.get(ID).getChildren();

	}


	public HashSet<String> getParents(String ID){
		return linkDB.get(ID).getParents();

	}
}

class Links implements Serializable{
	private HashSet<String> parents;
	private HashSet<String> children;
	Links(HashSet<String> P, HashSet<String> C){
		parents = P;
 		children = C;
	}
	public HashSet<String> getParents(){
		return parents;
	}
	public HashSet<String> getChildren(){
		return children;
	}
}
