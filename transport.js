var io = require('socket.io')
var clientIO = require('socket.io-client')
var fs = require('fs')
var kad = require('kad')
var shuffle = require('shuffle-array')
var _ = require('underscore')
var utils = require('./utils')

const checkAndRequestInterval = 4000

const approximateLimitOfNeighbours = 5

const limitOfInsufficientNeighboursTimes = 3

var Transport = function(id, port, streamMeta) {
	this._myID = id
	this._myPort = port
	this._streamMeta = streamMeta

	this._neighbours = []

	this._availablePackets = []		//My window of availability
	this._desiredPackets = []		//All desired packets, and in the neighbour's window of availability  
	this._outstandingPackets = []	//Outstanding packets being requested from neighbours
	this._myPackets = []			//Packets destinated to me 

	this._sequenceNumber = 0
	this._insufficientNeighboursTimes = 0

	this._createServer()
	this._checkNeighbourNumPeriodically()
	this._connectToNeighboursVoluntarily()
	this._checkAndRequestPeriodically()
}

Transport.prototype.sendToStream = function(data) {
	var metadata = this._createMetadata()
	var packet = {
		'header': metadata,
		'data': data
	}
	this._availablePackets.push(packet)

	this._notifyAllNeighbours(_.clone(metadata))
}


Transport.prototype._identifySocket = function(socketID) {
	var index = undefined

	for (var i = 0; i < this._neighbours.length; i++) {
		if (this._neighbours[i].id == socketID) {
			index = i
		}
	}

	return index
}

Transport.prototype._removeSocketByID = function(socketID) {
	var index = this._identifySocket(socketID)

	if (index != undefined) {
		this._neighbours.splice(index, 1)
	}
}

Transport.prototype._checkDuplicateSocketConnectionByID = function(socketID) {
	for (var i in this._neighbours) {
		if (this._neighbours[i].id == socketID) {
			return true
		}
	}
	return false
}

Transport.prototype._peerInfoResponse = function(res) {
	if (res.errMsg != undefined) {
    	console.log(res.errMsg)

    	this._removeSocketByID(res.socketID)
    } else {
    	this._handleInitialNotification(res)
    }
}


Transport.prototype._manageSocket = function(socket, peerID, forcefully) {
	var self = this

	socket.on('connect', function() {
	    console.log('connect')

	    socket.peerID = peerID

	    //There might be a case where this peer(acting as server) has
	    //already established a connection with the peer
	    //In this case, different from recovering from a reconnection, socket id is 
	    //different
	    if (self._checkDuplicateSocketConnectionByPeerID(peerID) && !self._checkDuplicateSocketConnectionByID(socket.id)) {
	    	socket.disconnect()
	    	return
	    }

	    //If a server recovers during reconnection attempts, we
	    //must avoid adding the duplicate socket.
	    //In another case where this peer acting as a client loses network
	    //connection, and then the network connection comes back, 
	    //this peer should also avoid adding the duplicate socket again.
	    if (!self._checkDuplicateSocketConnectionByID(socket.id)) {
	    	self._neighbours.push(socket)
	    }

	    //No matter whether this is a initial connection attempt or
	    //a reconnection attempt(server loses the socket info after crashing), 
	    //this peer should send its info to the peer server
	    //However, we don't send available packets now, because we don't know 
	    //whether this connection would be accepted by the other side
	    var peerInfo = {
	    	'peerID': self._myID,
	    	'forcefully': forcefully
	    }
	    socket.emit('peerInfo', peerInfo, self._peerInfoResponse.bind(self))
	})

	socket.on('reconnect_failed', function() {
        console.log('reconnect_failed')

        self._handleDisconnection(socket)
    })

    socket.on('notify', function(msg) {
    	self._handleNotify(msg)
    })

    socket.on('request', function(msg, callback) {
    	self._handleRequest(msg, callback)
    })
}

Transport.prototype._connectToNeighboursVoluntarily = function(forcefully) {
	var memberList = this._streamMeta.memberList
	shuffle(memberList)

	//Only try these amoount of times
	var connectTimes = approximateLimitOfNeighbours - this._neighbours.length

	for (var i = 0; i < connectTimes; i++) {
		if (this._neighbours.length >= approximateLimitOfNeighbours) {
			break
		}

		var member = memberList[i]
		var peerHost = member.host
		var peerPort = member.port
		var peerID = member.peerID

		if (peerID == this._myID) {
			continue
		}

		var options = {
			'reconnectionAttempts': 5,
			'reconnectionDelay': 4000
		}
		var URL = 'http://' + peerHost + ':' + peerPort
		var socket = clientIO.connect(URL, options)
		this._manageSocket(socket, peerID, forcefully)
	}
}

Transport.prototype._checkDuplicateSocketConnectionByPeerID = function(peerID) {
	for (var i in this._neighbours) {
		var socket = this._neighbours[i]
		var connectedPeer = socket.peerID

		if (peerID == connectedPeer) {
			return true
		}
	}
	return false
}

Transport.prototype._deletePacketsInOutstandingPackets = function(peerID) {
	var outstandingPackets = this._outstandingPackets
	var removePacketsFromIndex = []

	for (var i = 0; i < outstandingPackets.length; i++) {
		if (outstandingPackets[i].waitForPeer == peerID) {
			removePacketsFromIndex.push(i)
		}
	}

	utils.removeElementsFromArr(removePacketsFromIndex, outstandingPackets)
}

Transport.prototype._findIndexInIntermediatesOfDesiredPackets = function(intermediates, peerID) {
	for (var i = 0; i < intermediates.length; i++) {
		if (intermediates[i] == peerID) {
			return i
		}
	}
	return -1
}

Transport.prototype._checkAndDeletePacketsInDesiredPackets = function(peerID) {
	var desiredPackets = this._desiredPackets
	var removePacketsFromIndex = []

	for (var i = 0; i < desiredPackets.length; i++) {
		var index = this._findIndexInIntermediatesOfDesiredPackets(desiredPackets[i].intermediates, peerID)
		if (index == -1) {
			continue
		}
		if (desiredPackets[i].intermediates.length == 1) {
			removePacketsFromIndex.push(i)
		} else {
			desiredPackets[i].intermediates.splice(index, 1)
		}
	}

	utils.removeElementsFromArr(removePacketsFromIndex, desiredPackets)
}

//Once disconnecting, remove the socket immediately,
//but notice the sequence of operations here
Transport.prototype._handleDisconnection = function(socket) {
	this._deletePacketsInOutstandingPackets(socket.peerID)
	this._checkAndDeletePacketsInDesiredPackets(socket.peerID)
	this._removeSocketByID(socket.id)
}

//Notice that the initial notification message has 
//a list of available packets header without intermediate 
//field, because the response has a separate socketID field
Transport.prototype._generateInitialNotificationMsg = function() {
	var initialNotificationMsg = []

	for (var i in this._availablePackets) {
		initialNotificationMsg.push(this._availablePackets[i].header)
	}

	return initialNotificationMsg
}

Transport.prototype._handleInitialNotificationRes = function(peerAvailablePacketsMsg, peerID) {
	for (var i in peerAvailablePacketsMsg) {
		//Add intermediate to the packet just for the convenience of handling this packet
		peerAvailablePacketsMsg[i].intermediate = peerID
		this._handleNotify(peerAvailablePacketsMsg[i])
	}
}

Transport.prototype._handlePeerInfoAsServer = function(msg, serverSocket, callback) {
	var res = {
		'socketID': serverSocket.id
	}
	//If this is not a forceful connection and current number of neighbours is more than 
	//the limit number, this connection would be closed
	if (this._neighbours.length >= approximateLimitOfNeighbours && msg.forcefully == undefined) {
		res.errMsg = 'Sorry, exceed maximum connections'
		//Notice the sequence of callback and disconnect
		callback(res)
		serverSocket.disconnect()
	}
	//If this peer (acted as a client) has connected to the server voluntarily before, 
	//this peer would refuse the connection
	else if(this._checkDuplicateSocketConnectionByPeerID(msg.peerID)) {
		res.errMsg = 'Duplicate Socket Connection'
		//Notice the sequence of callback and disconnect
		callback(res)
		serverSocket.disconnect()
	} else {
		serverSocket.peerID = msg.peerID
		this._neighbours.push(serverSocket)

		//Notify the new neighbour all the available packets this peer has.
		//This method might cause unnecessary transmission of
		//all available packets info, if the connection between the client 
		//peer and the server peer is intermittent.
		//However, in the case of short network partitions, 
		//this transmission is also necessary
		res.availablePacketsMsg = this._generateInitialNotificationMsg()
		//myID is sent in the callback so that another side
		//can handle notify
		res.peerID = this._myID

		callback(res)
	}
}

Transport.prototype._checkNeighbourNum = function() {
	//Make sure each peer has neighbours > 2/3 of the limit 
	if (this._neighbours.length <= Math.ceil(approximateLimitOfNeighbours / 3 * 2)) {
		if (this._insufficientNeighboursTimes < limitOfInsufficientNeighboursTimes) {
			this._insufficientNeighboursTimes++
			this._connectToNeighboursVoluntarily()
		} else {
			//This is to cope with the case where a user cannot join the overlay network nicely or even cannot join, because
			//neighbours of other peers are saturated.
			//However, we should use this forceful method carefully. Use this method once, and then
			//reset insufficientNeighboursTimes to 0.
			this._insufficientNeighboursTimes = 0
			this._connectToNeighboursVoluntarily(true)
		}
	} else if (Math.ceil(approximateLimitOfNeighbours / 2) < this._neighbours.length && this._neighbours.length < approximateLimitOfNeighbours) {
		this._insufficientNeighboursTimes = 0
		this._connectToNeighboursVoluntarily()
	}
}

Transport.prototype._findIndexInAvailablePacketList = function(packetMsg) {
	var availablePackets = this._availablePackets

	for (var i = 0; i < availablePackets.length; i++) {
		if (availablePackets[i].header.source == packetMsg.source && availablePackets[i].header.sequenceNumber == packetMsg.sequenceNumber) {
			return i
		}
	}
	return -1
}

Transport.prototype._sendInitialNotification = function(peerAvailablePacketsMsg, socketID) {
	var availablePackets = this._availablePackets
	var initialNotificationList = []

	for (var i in availablePackets) {
		var index = utils.findIndexInPacketList(peerAvailablePacketsMsg, availablePackets[i].header)
		if (index == -1) {
			initialNotificationList.push(availablePackets[i].header)
		}	
	}

	var neighbourIndex = this._identifySocket(socketID)
	this._neighbours[neighbourIndex].emit('initialNotificationRes', initialNotificationList)
	
}

Transport.prototype._handleInitialNotification = function(initialNotification) {
	var peerID = initialNotification.peerID
	var socketID = initialNotification.socketID
	var availablePacketsMsg = initialNotification.availablePacketsMsg

	for (var i in availablePacketsMsg) {
		//Add intermediate to the packet just for the convenience of handling this packet
		availablePacketsMsg[i].intermediate = peerID
		this._handleNotify(availablePacketsMsg[i])
		delete availablePacketsMsg[i].intermediate
	}

	this._sendInitialNotification(availablePacketsMsg, socketID)
}

Transport.prototype._checkDesiredPackets = function(msg) {
	var desiredPackets = this._desiredPackets
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
Transport.prototype._handleNotify = function(msg) {
	var index = this._findIndexInAvailablePacketList(msg)
	
	if (index != -1) {
		return
	}

	this._checkDesiredPackets(msg)
}

Transport.prototype._notifyAllNeighbours = function(msg) {
	//This peer acts as an intermediate node for this packet
	msg.intermediate = this._myID

	for (var i in this._neighbours) {
		this._neighbours[i].emit('notify', msg)
	}
}

Transport.prototype._handleRequest = function(packetMsg, callback) {
	var index = this._findIndexInAvailablePacketList(packetMsg)

	if (index == -1) {
		callback('Error, no such packet')
	} else {
		callback(this._availablePackets[index])
	}
}

Transport.prototype._responseFromRequestData = function(data) {
	if (data == 'Error, no such packet') {
		console.log(data)
	} else {
		var indexInOutstandingPackets = utils.findIndexInPacketList(this._outstandingPackets, data.header)
		this._outstandingPackets.splice(indexInOutstandingPackets, 1)

		var indexInDesiredPackets = utils.findIndexInPacketList(this._desiredPackets, data.header)
		this._desiredPackets.splice(indexInDesiredPackets, 1)

		this._availablePackets.push(data)

		this._notifyAllNeighbours(_.clone(data.header))
	}
}

Transport.prototype._Request = function(packet) {
	// if (packet.intermediates.length == 1) {
		var neighbours = this._neighbours
		var requestingPacket = utils.deepCloneObject(packet)

		for (var i in neighbours) {
			if (neighbours[i].peerID == packet.intermediates[0]) {
				delete requestingPacket.intermediates

				neighbours[i].emit('request', requestingPacket, this._responseFromRequestData.bind(this))

				requestingPacket.waitForPeer = neighbours[i].peerID
				this._outstandingPackets.push(requestingPacket)
			}
		}
	// }
}

//Request all the packets that are in the desired packet list, 
//but not in the outstanding packet list
Transport.prototype._checkAndRequest = function() {
	var desiredPackets = this._desiredPackets

	for (var i in desiredPackets) {
		//Avoid sending requests for the same packet to multiple peers
		var index = utils.findIndexInPacketList(this._outstandingPackets, desiredPackets[i])

		if (index != -1) {
			continue
		}

		this._Request(utils.deepCloneObject(desiredPackets[i]))
	}
}

Transport.prototype._checkAndRequestPeriodically = function() {
	setInterval(this._checkAndRequest.bind(this), checkAndRequestInterval)
}

//Add communication overlay metadata
Transport.prototype._createMetadata = function() {
	var metadata = {
		'timestamp': new Date(),
		'source': this._myID,
		'destination': 'all',
		'sequenceNumber': this._sequenceNumber++
	}

	return metadata
}

Transport.prototype._createServer = function() {
	var self = this

	var serverIO = io(this._myPort)

	serverIO.on('connection', function (serverSocket) {
		serverSocket.on('notify', function (msg) {
			self._handleNotify(msg)
		})
		serverSocket.on('request', function (msg, callback) {
			self._handleRequest(msg, callback)
		})
		serverSocket.on('peerInfo', function (msg, callback) {
			self._handlePeerInfoAsServer(msg, serverSocket, callback)
		})
		serverSocket.on('initialNotificationRes', function (msg) {
			self._handleInitialNotificationRes(msg, serverSocket.peerID)
		})
		//The other side might be down or, 
		//this peer (acting as a server) loses partial network
		//connection, but can still connection to
		//some neighbours (network partition) or,
		//this peer loses all network connections
		serverSocket.on('disconnect', function () {
			self._handleDisconnection(serverSocket)
		})
	})
}

Transport.prototype._checkNeighbourNumPeriodically = function() {
	setInterval(this._checkNeighbourNum.bind(this), 10000)
}

module.exports = Transport