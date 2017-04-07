#!/bin/bash

numOfPeers=20
basePort=10000
difference=100
baseLogPathName="./logs/log"

for (( i = 0;  i < $numOfPeers;  i++ ))
do
	port=`expr $basePort + $i \* $difference`
	fileName=$baseLogPathName$i
	echo "******** Restart ********" >> $fileName
	node test/restore_20.js $port >> $fileName &
done