#!/bin/bash

java  -cp ./lib/htmlparser.jar:./lib/rocksdbjni-6.9.0-linux64.jar:./ $1 "$2" $3
