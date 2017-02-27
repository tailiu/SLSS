var io = require('socket.io')
var clientIO = require('socket.io-client')
var fs = require('fs')
var crypto = require('crypto')
var kad = require('kad')
var shuffle = require('shuffle-array')
var _ = require('underscore')

const serverIOPort = process.argv[2]

const checkAndRequestInterval = 4000

const approximateLimitOfNeighbours = 5

const limitOfInsufficientNeighboursTimes = 3

const DHTPort = 9000
const DHTAddr = 'localhost'
const DHTdb = 'db2'
const DHTSeed = {
	address: '127.0.0.1',
	port: 8200
}

//For test only
const numOfPeers = 50
const basePort = 10000
const difference = 100

var sequenceNumber = 0

var insufficientNeighboursTimes = 0

var streamMeta = {}

var neighbours = []

function createStreamMeta() {
	streamMeta.memberList = []

	for (var i = 0; i < numOfPeers; i++) {
		var peerAddr = {
			'host': 'localhost',
			'port': basePort + difference * i,
			'peerID': createHash((basePort + difference * i) + '')
		}
		streamMeta.memberList.push(peerAddr)
	}
}



function createDHTNode(nodeAddr, nodePort, db) {
	var DHTNode = new kad.Node({
		transport: kad.transports.UDP(kad.contacts.AddressPortContact({
			address: nodeAddr,
			port: nodePort
		})),
		storage: kad.storage.FS(db)
	})
	return DHTNode
}

function putToDHT(DHTNode, DHTSeed, key, value, callback) {
	DHTNode.connect(DHTSeed, function(err) {
		DHTNode.put(key, value, function() {
			callback()
		})
	})
}

function getFromDHT(DHTNode, DHTSeed, key, callback) {
	DHTNode.connect(DHTSeed, function(err) {
		DHTNode.get(key, function(err, value) {
			callback(value)
		})
	})
}





function identifySocket(socketID) {
	var index = undefined

	for (var i = 0; i < neighbours.length; i++) {
		if (neighbours[i].id == socketID) {
			index = i
		}
	}

	return index
}

function removeSocketByID(socketID) {
	var index = identifySocket(socketID)

	if (index != undefined) {
		neighbours.splice(index, 1)
	}
}

function checkDuplicateSocketConnectionByID(socketID) {
	for (var i in neighbours) {
		if (neighbours[i].id == socketID) {
			return true
		}
	}
	return false
}

function peerInfoResponse(res) {
	if (res.errMsg != undefined) {
    	console.log(res.errMsg)

    	removeSocketByID(res.socketID)
    } else {
    	handleInitialNotification(res)
    }
}


function manageSocket(socket, peerID, forcefully) {
	socket.on('connect', function() {
	    console.log('connect')

	    socket.peerID = peerID

	    //There might be a case where this peer(acting as server) has
	    //already established a connection with the peer
	    //In this case, different from recovering from a reconnection, socket id is 
	    //different
	    if (checkDuplicateSocketConnectionByPeerID(peerID) && !checkDuplicateSocketConnectionByID(socket.id)) {
	    	socket.disconnect()
	    	return
	    }

	    //If a server recovers during reconnection attempts, we
	    //must avoid adding the duplicate socket.
	    //In another case where this peer acting as a client loses network
	    //connection, and then the network connection comes back, 
	    //this peer should also avoid adding the duplicate socket again.
	    if (!checkDuplicateSocketConnectionByID(socket.id)) {
	    	neighbours.push(socket)
	    }

	    //No matter whether this is a initial connection attempt or
	    //a reconnection attempt(server loses the socket info after crashing), 
	    //this peer should send its info to the peer server
	    //However, we don't send available packets now, because we don't know 
	    //whether this connection would be accepted by the other side
	    var peerInfo = {
	    	'peerID': myID,
	    	'forcefully': forcefully
	    }
	    socket.emit('peerInfo', peerInfo, peerInfoResponse)
	})

	socket.on('reconnect_failed', function() {
        console.log('reconnect_failed')

        handleDisconnection(socket)
    })

    socket.on('notify', function(msg) {
    	HandleNotify(msg)
    })

    socket.on('request', function(msg, callback) {
    	HandleRequest(msg, callback)
    })
}

function getPeerURL(host, port) {
	return 'http://' + host + ':' + port
}

function connectToNeighboursVoluntarily(memberList, forcefully) {
	shuffle(memberList)

	//Only try these amoount of times
	var connectTimes = approximateLimitOfNeighbours - neighbours.length

	for (var i = 0; i < connectTimes; i++) {
		if (neighbours.length >= approximateLimitOfNeighbours) {
			break
		}

		var member = memberList[i]
		var peerHost = member.host
		var peerPort = member.port
		var peerID = member.peerID

		if (peerID == myID) {
			continue
		}

		var options = {
			'reconnectionAttempts': 5,
			'reconnectionDelay': 4000
		}
		var socket = clientIO.connect(getPeerURL(peerHost, peerPort), options)
		manageSocket(socket, peerID, forcefully)
	}
}

function checkDuplicateSocketConnectionByPeerID(peerID) {
	for (var i in neighbours) {
		var socket = neighbours[i]
		var connectedPeer = socket.peerID

		if (peerID == connectedPeer) {
			return true
		}
	}
	return false
}

//Notice: the indexArr should contain index in the ascending order
function removeElementsFromArr(indexArr, arr) {
	for (var i = indexArr.length - 1; i >= 0; i--) {
		arr.splice(indexArr[i], 1)
	}
}

function deletePacketsInOutstandingPackets(peerID) {
	var removePacketsFromIndex = []

	for (var i = 0; i < outstandingPackets.length; i++) {
		if (outstandingPackets[i].waitForPeer == peerID) {
			removePacketsFromIndex.push(i)
		}
	}

	removeElementsFromArr(removePacketsFromIndex, outstandingPackets)
}

function findIndexInIntermediatesOfDesiredPackets(intermediates, peerID) {
	for (var i = 0; i < intermediates.length; i++) {
		if (intermediates[i] == peerID) {
			return i
		}
	}
	return -1
}

function checkAndDeletePacketsInDesiredPackets(peerID) {
	var removePacketsFromIndex = []

	for (var i = 0; i < desiredPackets.length; i++) {
		var index = findIndexInIntermediatesOfDesiredPackets(desiredPackets[i].intermediates, peerID)
		if (index == -1) {
			continue
		}
		if (desiredPackets[i].intermediates.length == 1) {
			removePacketsFromIndex.push(i)
		} else {
			desiredPackets[i].intermediates.splice(index, 1)
		}
	}

	removeElementsFromArr(removePacketsFromIndex, desiredPackets)
}

//Once disconnecting, remove the socket immediately,
//but notice the sequence of operations here
function handleDisconnection(socket) {
	deletePacketsInOutstandingPackets(socket.peerID)
	checkAndDeletePacketsInDesiredPackets(socket.peerID)
	removeSocketByID(socket.id)
}

//Notice that the initial notification message has 
//a list of available packets header without intermediate 
//field, because the response has a separate socketID field
function generateInitialNotificationMsg() {
	var initialNotificationMsg = []

	for (var i in availablePackets) {
		initialNotificationMsg.push(availablePackets[i].header)
	}

	return initialNotificationMsg
}

function handleInitialNotificationRes(peerAvailablePacketsMsg, peerID) {
	for (var i in peerAvailablePacketsMsg) {
		//Add intermediate to the packet just for the convenience of handling this packet
		peerAvailablePacketsMsg[i].intermediate = peerID
		HandleNotify(peerAvailablePacketsMsg[i])
	}
}

function handlePeerInfoAsServer(msg, serverSocket, callback) {
	var res = {
		'socketID': serverSocket.id
	}
	//If this is not a forceful connection and current number of neighbours is more than 
	//the limit number, this connection would be closed
	if (neighbours.length >= approximateLimitOfNeighbours && msg.forcefully == undefined) {
		res.errMsg = 'Sorry, exceed maximum connections'
		//Notice the sequence of callback and disconnect
		callback(res)
		serverSocket.disconnect()
	}
	//If this peer (acted as a client) has connected to the server voluntarily before, 
	//this peer would refuse the connection
	else if(checkDuplicateSocketConnectionByPeerID(msg.peerID)) {
		res.errMsg = 'Duplicate Socket Connection'
		//Notice the sequence of callback and disconnect
		callback(res)
		serverSocket.disconnect()
	} else {
		serverSocket.peerID = msg.peerID
		neighbours.push(serverSocket)

		//Notify the new neighbour all the available packets this peer has.
		//This method might cause unnecessary transmission of
		//all available packets info, if the connection between the client 
		//peer and the server peer is intermittent.
		//However, in the case of short network partitions, 
		//this transmission is also necessary
		res.availablePacketsMsg = generateInitialNotificationMsg()
		//myID is sent in the callback so that another side
		//can handle notify
		res.peerID = myID

		callback(res)
	}
}

function checkNeighbourNum() {
	//Make sure each peer has neighbours > 2/3 of the limit 
	if (neighbours.length <= Math.ceil(approximateLimitOfNeighbours / 3 * 2)) {
		if (insufficientNeighboursTimes < limitOfInsufficientNeighboursTimes) {
			insufficientNeighboursTimes++
			connectToNeighboursVoluntarily(streamMeta.memberList)
		} else {
			//This is to cope with the case where a user cannot join the overlay network nicely or even cannot join, because
			//neighbours of other peers are saturated.
			//However, we should use this forceful method carefully. Use this method once, and then
			//reset insufficientNeighboursTimes to 0.
			insufficientNeighboursTimes = 0
			connectToNeighboursVoluntarily(streamMeta.memberList, true)
		}
	} else if (Math.ceil(approximateLimitOfNeighbours / 2) < neighbours.length && neighbours.length < approximateLimitOfNeighbours) {
		insufficientNeighboursTimes = 0
		connectToNeighboursVoluntarily(streamMeta.memberList)
	}
}



var availablePackets = []		//My window of availability
var desiredPackets = []			//All desired packets, and in the neighbour's window of availability  
var outstandingPackets = []		//Outstanding packets being requested from neighbours
var myPackets = []				//Packets destinated to me 

function findIndexInAvailablePacketList(packetMsg) {
	for (var i = 0; i < availablePackets.length; i++) {
		if (availablePackets[i].header.source == packetMsg.source && availablePackets[i].header.sequenceNumber == packetMsg.sequenceNumber) {
			return i
		}
	}
	return -1
}

function sendInitialNotification(peerAvailablePacketsMsg, socketID) {
	var initialNotificationList = []

	for (var i in availablePackets) {
		var index = findIndexInPacketList(peerAvailablePacketsMsg, availablePackets[i].header)
		if (index == -1) {
			initialNotificationList.push(availablePackets[i].header)
		}	
	}

	var neighbourIndex = identifySocket(socketID)
	neighbours[neighbourIndex].emit('initialNotificationRes', initialNotificationList)
	
}

function handleInitialNotification(initialNotification) {
	var peerID = initialNotification.peerID
	var socketID = initialNotification.socketID
	var availablePacketsMsg = initialNotification.availablePacketsMsg

	for (var i in availablePacketsMsg) {
		//Add intermediate to the packet just for the convenience of handling this packet
		availablePacketsMsg[i].intermediate = peerID
		HandleNotify(availablePacketsMsg[i])
		delete availablePacketsMsg[i].intermediate
	}

	sendInitialNotification(availablePacketsMsg, socketID)
}

function checkDesiredPackets(msg) {
	var intermediate = msg.intermediate

	for (var i in desiredPackets) {
		if (desiredPackets[i].source == msg.source && desiredPackets[i].sequenceNumber == msg.sequenceNumber) {
			desiredPackets[i].intermediates.push(intermediate)
			return
		}
	}

	desiredPackets.push(msg)
	var length = desiredPackets.length
	desiredPackets[length - 1].intermediates = []
	desiredPackets[length - 1].intermediates.push(intermediate)
	delete desiredPackets[length - 1].intermediate
}

//For now, we handle notify by looking at 
//availablePackets and desiredPackets
function HandleNotify(msg) {
	var index = findIndexInAvailablePacketList(msg)
	
	if (index != -1) {
		return
	}

	checkDesiredPackets(msg)
}

function notifyAllNeighbours(msg) {
	//This peer acts as an intermediate node for this packet
	msg.intermediate = myID

	for (var i in neighbours) {
		neighbours[i].emit('notify', msg)
	}
}

function HandleRequest(packetMsg, callback) {
	var index = findIndexInAvailablePacketList(packetMsg)

	if (index == -1) {
		callback('Error, no such packet')
	} else {
		callback(availablePackets[index])
	}
}

function findIndexInPacketList(packets, packetMsg) {
	for (var i = 0; i < packets.length; i++) {
		if (packets[i].source == packetMsg.source && packets[i].sequenceNumber == packetMsg.sequenceNumber) {
			return i
		}
	}
	return -1
}

function responseFromRequestData(data) {
	if (data == 'Error, no such packet') {
		console.log(data)
	} else {
		var indexInOutstandingPackets = findIndexInPacketList(outstandingPackets, data.header)
		outstandingPackets.splice(indexInOutstandingPackets, 1)

		var indexInDesiredPackets = findIndexInPacketList(desiredPackets, data.header)
		desiredPackets.splice(indexInDesiredPackets, 1)

		availablePackets.push(data)

		notifyAllNeighbours(_.clone(data.header))
	}
}


function deepCloneObject(object) {
	return JSON.parse(JSON.stringify(object))
}

function Request(packet) {
	// if (packet.intermediates.length == 1) {
		var requestingPacket = deepCloneObject(packet)

		for (var i in neighbours) {
			if (neighbours[i].peerID == packet.intermediates[0]) {
				delete requestingPacket.intermediates

				neighbours[i].emit('request', requestingPacket, responseFromRequestData)

				requestingPacket.waitForPeer = neighbours[i].peerID
				outstandingPackets.push(requestingPacket)
			}
		}
	// }
}

//Request all the packets that are in the desired packet list, 
//but not in the outstanding packet list
function checkAndRequest() {
	for (var i in desiredPackets) {
		//Avoid sending requests for the same packet to multiple peers
		var index = findIndexInPacketList(outstandingPackets, desiredPackets[i])

		if (index != -1) {
			continue
		}

		Request(deepCloneObject(desiredPackets[i]))
	}
}

setInterval(checkAndRequest, checkAndRequestInterval)

//Add communication overlay metadata
function createMetadata() {
	var metadata = {
		'timestamp': new Date(),
		'source': myID,
		'destination': 'all',
		'sequenceNumber': sequenceNumber++
	}

	return metadata
}

function sendToStream(data) {
	var metadata = createMetadata()
	var packet = {
		'header': metadata,
		'data': data
	}
	availablePackets.push(packet)

	notifyAllNeighbours(_.clone(metadata))
}




function createHash(dataToHash) {
	var hash = crypto.createHash('sha256')
	hash.update(dataToHash)
	return hash.digest('hex')
}

function createRandom() {
	var current_date = (new Date()).valueOf().toString()
	var random = Math.random().toString()
	return createHash(current_date + random)
}

//For now for testing purpose, we only use each unique port number as
//peer ID, but we can replace the port number with a random number generated
//by createRandom() in the future
function createMyID() {
	return createHash(serverIOPort + '')
}

var myID = createMyID()




var serverIO = io(serverIOPort)

serverIO.on('connection', function (serverSocket) {
	serverSocket.on('notify', function (msg) {
		HandleNotify(msg)
	})
	serverSocket.on('request', function (msg, callback) {
		HandleRequest(msg, callback)
	})
	serverSocket.on('peerInfo', function (msg, callback) {
		handlePeerInfoAsServer(msg, serverSocket, callback)
	})
	serverSocket.on('initialNotificationRes', function (msg) {
		handleInitialNotificationRes(msg, serverSocket.peerID)
	})
	//The other side might be down or, 
	//this peer (acting as a server) loses partial network
	//connection, but can still connection to
	//some neighbours (network partition) or,
	//this peer loses all network connections
	serverSocket.on('disconnect', function () {
		handleDisconnection(serverSocket)
	})
})

createStreamMeta()

//var DHTNode = createDHTNode(DHTAddr, DHTPort, DHTdb)

//getFromDHT(DHTNode, DHTSeed, 'test', function(streamMeta){
	connectToNeighboursVoluntarily(streamMeta.memberList)
//})

setInterval(checkNeighbourNum, 10000)




function writeToLog() {
	console.log('****************** One Log Start *****************************')
	console.log('Me: ' + myID)
	console.log('My neighbours:')
	for (var i in neighbours) {
		console.log(neighbours[i].peerID)
	}
	console.log('Outstanding Packets:')
	console.log(outstandingPackets)
	console.log('Desired Packets:')
	console.log(desiredPackets)
	console.log('Available Packets:')
	console.log(availablePackets)
	console.log('Length of Available Packets: ' + availablePackets.length)

	var logTime = new Date()
	var elapsedTime = logTime - startTime
	console.log('Elapsed Time: ' + elapsedTime + ' ms')
	console.log('******************** One Log End ***************************')
}

setInterval(writeToLog, 5000)




const numOfPackets = 10
const sendInterval = 5000

function sendData() {
	sendToStream('A data packet from peer ' + myID)
}

var startTime = new Date()

// setInterval(sendData, 5000)

// setTimeout(sendData, 5000)

// for (var i = 0; i < numOfPackets; i++) {
// 	setTimeout(sendData, sendInterval * i)
// }

for (var i = 0; i < numOfPackets; i++) {
	sendData()
}
