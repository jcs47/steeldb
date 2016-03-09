#/bin/bash

CLIENT_ID=$1

java -Ddivdb.firstclient=$CLIENT_ID -Dlog4j.configuration=file:config/log4j.xml -cp bin/*:lib/* lasige.steeldb.demo.console.DBConsole
