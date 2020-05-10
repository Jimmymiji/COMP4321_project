Before Start:
	mkdir db
	mkdir lib
	move htmlparser.jar  rocksdbjni-6.8.0-linux64.jar to folder lib

when wrok on web server:
    clone this repo to web-app folder in apache home,
    do the steps in Before Start
    mkdir WEB-INF
    mkdir WEB-INF/classes
    cp -r comp4321 WEB-INF/classes
    cp -r IRUtilities WEB-INF/classes
    cp -r lib WEB-INF
    cp src/projectMain.* ./
    
    access the webpage by entering  "IPOFYOURSERVER:8080/comp4321/projectMain.html" in your browser


To compile:
javac -d ./ -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   src/*.java

To run spider:
java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   comp4321.Spider

To test indexer:
java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   comp4321.InvertedIndex

To test engine:
java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   comp4321.Engine   "QUERY"   num_of_results_you_want

To run our test program:
java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   comp4321.Test > spider_result.txt
