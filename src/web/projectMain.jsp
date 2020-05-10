<%@ page import="java.io.*,java.util.*,org.rocksdb.RocksDB,org.rocksdb.RocksDBException" %>
<%@ page import="comp4321.Engine" %>
<%!

Engine engine;
ArrayList<HashMap <String,String> > tempRes = new ArrayList<HashMap<String,String>>();
String s;
String n;
boolean init = false;

public void jspInit(){
	RocksDB.loadLibrary();	
}

public void jspDestroy(){
   engine = null;
   tempRes = null;
   s = null;
   init = false;
}

%>
<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1252"><title>COMP4321 Simple Search Engine</title></head><body>
<% 
s = request.getParameter("query"); 
n = request.getParameter("number");
%>

<h3>Your Query is : <%= s %> </h3><br>
<h3>Number of Results You want to display : <%= n %> </h3><br>

<h1>Search Results :  </h1><br>


<% 
if(!init){
	try{
		engine = new Engine("../webapps/comp4321/db/db1","../webapps/comp4321/db/db2","../webapps/comp4321/db/db3","../webapps/comp4321/db/db4","../webapps/comp4321/db/db5", "../webapps/comp4321/db/db6", "../webapps/comp4321/db/db7", "../webapps/comp4321/db/db8","../webapps/comp4321/db/db","../webapps/comp4321/linkdb");
	}catch(RocksDBException e)
	{
        out.print("Error when init search engine <br>");
		out.print(e.toString() + "<br>");
		out.print(System.getProperty("user.dir") + "<br>");
	}
	init = true;
}

ArrayList<HashMap <String,String> > tempRes = engine.retrieve(s,Integer.parseInt(n));
if( tempRes != null){

    for(HashMap<String, String> tpage : tempRes){
        
        String pageScore = tpage.get("score");
        if(pageScore == null)	pageScore = "Not Avaliable";
        String pageUrl = tpage.get("url");
        String pageTitle = tpage.get("title");
        if(pageTitle == null)   pageTitle = "Not Avaliable";
        String pageDate = tpage.get("date");
        if(pageDate == null)    pageDate = "Not Avaliable";
        String pageSize = tpage.get("pagesize");
        if(pageSize == null)    pageSize = "Not Avaliable";
        String pageKeyFreq = tpage.get("key_freq");
        if(pageKeyFreq == null) pageKeyFreq = "Not Avaliable";
        
        String[] pageChildren;
        String pageChild= tpage.get("children");
        if(pageChild!=null){
            if(pageChild.indexOf(";") != -1){
                pageChildren = pageChild.split(";");
            }else{
                pageChildren = new String[1];
                pageChildren[0] = pageChild;
            }
        }else{
            pageChildren = new String[1];
            pageChildren[0] = new String("Not Avaliable");
        }

        String[] pageParents;
        String pageParent = tpage.get("parents");
        if(pageParent!=null){
            if(pageParent.indexOf(";") != -1){
                pageParents = pageChild.split(";");
            }else{
                pageParents = new String[1];
                pageParents[0] = pageParent;
            }
        }else{
            pageParents = new String[1];
            pageParents[0] = new String("Not Avaliable");
        }
        
        out.print("Score: "+pageScore + "<br><br>");
        if(pageUrl!=null){
            out.print("URL: "+"<a href=\"" + pageUrl + "\"> "+pageUrl+"</a><br><br>");
        }else{
            out.print("URL: Not Avaliable <br><br>");
        }
        out.print("Title: "+pageTitle + "<br><br>");
        out.print("Date: "+pageDate + "<br><br>");
        out.print("Size: "+pageSize + "<br><br>");
        out.print("Key words: "+pageKeyFreq + "<br><br>");
        out.print("Page Children: <br>");
        for(String s : pageChildren){
            if(s!=""){
                out.print("<a href=\"" + s + "\"> "+s+"</a><br/>");
            }else{
                out.print("Not Avaliable"+"<br>");
            }
        }
        out.print("<br>");
        out.print("Page Parents: <br>");
        for(String s : pageParents){
            if(s!=""){
                out.print("<a href=\"" + s + "\"> "+s+"</a><br/>");
            }else{
                out.print("Not Avaliable"+"<br>");
            }
        }
        
        out.print("<br> ============================================  <br><br>");
        }
    }
%>


</body></html>