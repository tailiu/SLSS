var io = require('socket.io')
var clientIO = require('socket.io-client')
var fs = require('fs')
var crypto = require('crypto')
var kad = require('kad')
var shuffle = require('shuffle-array')
var _ = require('underscore')

const serverIOPort = process.argv[2]

const checkAndRequestInterval = 5000

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
			'port': basePort + difference * i
		}
		streamMeta.memberList.push(peerAddr)
	}
}

function createHash(dataToHash) {
	var hash = crypto.createHash('sha256')
	hash.update(dataToHash)
	return hash.digest('hex')
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

function checkDuplicateSocketConnectionByID(socket) {
	for (var i in neighbours) {
		if (neighbours[i].id == socket.id) {
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


function manageSocket(socket, host, port, forcefully) {
	socket.on('connect', function() {
	    console.log('connect')

	    socket.host = host
	    socket.port = port

	    //There might be a case where this peer(acting as server) has
	    //already established a connection with the peer with this host and port
	    //In this case, different from recovering from a reconnection, socket id is 
	    //different
	    if (checkDuplicateSocketConnectionByAddr(host, port) && !checkDuplicateSocketConnectionByID(socket)) {
	    	socket.disconnect()
	    	return
	    }

	    //If a server recovers during reconnection attempts, we
	    //must avoid adding the duplicate socket.
	    //In another case where this peer acting as a client loses network
	    //connection, and then the network connection comes back, 
	    //this peer should also avoid adding the duplicate socket again.
	    if (!checkDuplicateSocketConnectionByID(socket)) {
	    	neighbours.push(socket)
	    }

	    //No matter whether this is a initial connection attempt or
	    //a reconnection attempt(server loses the socket info after crashing), 
	    //this peer should send its info to the peer server
	    //However, we don't send available packets now, because we don't know 
	    //whether this connection would be accepted by the other side
	    var peerInfo = {
	    	'host': 'localhost',
	    	'port': serverIOPort,
	    	'forcefully': forcefully
	    }
	    socket.emit('peerInfo', peerInfo, peerInfoResponse)
	})

	socket.on('reconnect_failed', function() {
        console.log('reconnect_failed')

        removeSocketByID(socket.id)
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

	//Only try these times
	var connectTimes = approximateLimitOfNeighbours - neighbours.length

	for (var i = 0; i < connectTimes; i++) {
		if (neighbours.length >= approximateLimitOfNeighbours) {
			break
		}

		var member = memberList[i]
		var host = member.host
		var port = member.port

		if (host == 'localhost' && port == serverIOPort) {
			continue
		}

		var options = {
			'reconnectionAttempts': 5,
			'reconnectionDelay': 4000
		}
		var socket = clientIO.connect(getPeerURL(host, port), options)
		manageSocket(socket, host, port, forcefully)
	}
}

function checkDuplicateSocketConnectionByAddr(host, port) {
	for (var i in neighbours) {
		var socket = neighbours[i]
		var socketHost = socket.host
		var socketPort = socket.port

		if (host == socketHost && port == socketPort) {
			return true
		}
	}
	return false
}

//Once disconnecting, remove the socket immediately
function handleDisconnectionAsServer(serverSocket) {
	removeSocketByID(serverSocket.id)
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

function handleInitialNotificationRes(peerAvailablePacketsMsg, peerAddr) {
	for (var i in peerAvailablePacketsMsg) {
		//Add intermediate to the packet just for the convenience of handling this packet
		peerAvailablePacketsMsg[i].intermediate = peerAddr
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
		callback(res)
		serverSocket.disconnect()
	}
	//If this peer (acted as a client) has connected to the server voluntarily before, 
	//this peer would refuse the connection
	else if(checkDuplicateSocketConnectionByAddr(msg.host, msg.port)) {
		res.errMsg = 'Duplicate Socket Connection'
		callback(res)
		serverSocket.disconnect()
	} else {
		serverSocket.host = msg.host
		serverSocket.port = msg.port
		neighbours.push(serverSocket)

		//Notify the new neighbour all the available packets this peer has.
		//This method might cause unnecessary transmission of
		//all available packets info, if the connection between the client 
		//peer and the server peer is intermittent.
		//However, in the case of short network partitions, 
		//this transmission is also necessary
		res.availablePacketsMsg = generateInitialNotificationMsg()
		//serverIOPort is sent in the callback so that another side
		//can handle notify
		res.peerAddr = serverIOPort

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
var myPackets = []				//Packets destinated to me   
var outstandingPackets = []		//Outstanding packets being requested from neighbours

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
	var peerAddr = initialNotification.peerAddr
	var socketID = initialNotification.socketID
	var availablePacketsMsg = initialNotification.availablePacketsMsg

	for (var i in availablePacketsMsg) {
		//Add intermediate to the packet just for the convenience of handling this packet
		availablePacketsMsg[i].intermediate = peerAddr
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
	msg.intermediate = serverIOPort

	for (var i in neighbours) {
		neighbours[i].emit('notify', msg)
	}
}

function createMetadata() {
	var metadata = {
		'timestamp': new Date(),
		'source': serverIOPort,
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
			if (neighbours[i].port == packet.intermediates[0]) {
				delete requestingPacket.intermediates

				neighbours[i].emit('request', requestingPacket, responseFromRequestData)

				requestingPacket.waitForPeer = neighbours[i].port
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
		handleInitialNotificationRes(msg, serverSocket.port)
	})
	//The other side might be down or, 
	//this peer acting as a server loses partial network
	//connection(network partition), but can still connection to
	//some neighbours or,
	//this peer loses all network connections
	serverSocket.on('disconnect', function () {
		handleDisconnectionAsServer(serverSocket)
	})
})

createStreamMeta()

//var DHTNode = createDHTNode(DHTAddr, DHTPort, DHTdb)

//getFromDHT(DHTNode, DHTSeed, 'test', function(streamMeta){
	connectToNeighboursVoluntarily(streamMeta.memberList)
//})

setInterval(checkNeighbourNum, 10000)




function writeToLog() {
	console.log('peer ' + serverIOPort)
	for (var i in neighbours) {
		console.log(neighbours[i].port)
	}
	console.log(outstandingPackets)
	console.log(desiredPackets)
	console.log(availablePackets)
	console.log(availablePackets.length)
}

setInterval(writeToLog, 5000)




const numOfPackets = 10
const sendInterval = 5000

function sendData() {
	sendToStream('kkkkkkk ' + serverIOPort)
}

// setInterval(sendData, 5000)

// setTimeout(sendData, 5000)

for (var i = 0; i < numOfPackets; i++) {
	setTimeout(sendData, sendInterval * i)
}
