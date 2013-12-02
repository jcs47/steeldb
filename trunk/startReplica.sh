#!/bin/bash

REPLICA_INDEX=$1

java -cp bin/:lib/* -Dreplicaid=$REPLICA_INDEX -Duser.timezone=UTC -Dlog4j.configuration=file:config/log4j.xml lasige.steeldb.Replica.InitReplica config/replica$REPLICA_INDEX.properties
