#!/bin/bash

REPLICA_INDEX=$1
MASTER=$2

#java -cp ../../SteelDB/dist/*:../../SteelDB/dist/lib/* -Dreplicaid=$REPLICA_INDEX -Duser.timezone=UTC -Dlog4j.configuration=file:config/log4j.xml lasige.steeldb.Replica.InitReplica config/replica$REPLICA_INDEX.properties

java -cp ./bin/*:./lib/* -Dreplicaid=$REPLICA_INDEX -Duser.timezone=UTC -Dlog4j.configuration=file:config/log4j.xml lasige.steeldb.Replica.InitReplica config/replica$REPLICA_INDEX.properties $MASTER

#java -cp ./bin/*:./lib/*:../../BFT-SMaRt/dist/BFT-SMaRt.jar:../library/lib/:../../JThreshSig/dist/JThreshSig.jar -Dreplicaid=$REPLICA_INDEX -Duser.timezone=UTC -Dlog4j.configuration=file:config/log4j.xml lasige.steeldb.Replica.InitReplica config/replica$REPLICA_INDEX.properties
