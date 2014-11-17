#!/bin/bash
REMOTE_CFG=./config-remote 
SNAPSHOT=__snapshot.json
./cli.js --config $REMOTE_CFG --snapshot > $SNAPSHOT
./cli.js --recreate ./$SNAPSHOT
rm $SNAPSHOT
