#!/bin/bash

numOfPeers=50
basePort=10000
difference=100
baseLogPathName="./logs/log"

# rm -r ./log_system/*

for (( i = 0;  i < $numOfPeers;  i++ ))
do
	port=`expr $basePort + $i \* $difference`
	fileName=$baseLogPathName$i
	echo "******** Restart ********" >> $fileName
	node test/restart_all.js $port >> $fileName &
done
