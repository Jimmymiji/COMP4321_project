To compile:
javac -d ./ -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   src/*.java
To run spider:
java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   comp4321.Spider
To test indexer:
java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.8.0-linux64.jar:./   comp4321.InvertedIndexer
