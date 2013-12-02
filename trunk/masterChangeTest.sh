#!/bin/bash

java -Duser.timezone=UTC -Dlog4j.configuration=file:config/log4j.xml -cp bin/:lib/* lasige.steeldb.test.MasterChangeTest
