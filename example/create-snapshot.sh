#!/bin/bash
REMOTE_CFG=./config-remote 
export SNAPSHOT=__snapshot.json
./cli.js --config $REMOTE_CFG --snapshot > $SNAPSHOT
