#!/bin/bash

numOfPeers=50
basePort=10000
difference=100
baseLogPathName="./logs/log"

for (( i = 0;  i < $numOfPeers;  i++ ))
do
	port=`expr $basePort + $i \* $difference`
	fileName=$baseLogPathName$i
	node stencil.js $port > $fileName &
done
