var transport = require('./transport')
var crypto = require('crypto')

var Stencil = function(id, port, streamMeta) {
	this.id = id
	this.port = port
	this.streamMeta = streamMeta

	this.transport = new transport(id, port, streamMeta)
}

Stencil.prototype.sendToStream = function(data) {
	this.transport.sendToStream(data)
}

//For test only
const numOfPeers = 50
const basePort = 10000
const difference = 100

const serverIOPort = process.argv[2]

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
	return serverIOPort + ''
}

var myID = createMyID()

var streamMeta = {}

function createStreamMeta() {
	

	streamMeta.memberList = []

	for (var i = 0; i < numOfPeers; i++) {
		var peerAddr = {
			'host': 'localhost',
			'port': basePort + difference * i,
			'peerID': (basePort + difference * i) + ''
		}
		streamMeta.memberList.push(peerAddr)
	}

}

createStreamMeta()


const numOfPackets = 10


var stencil = new Stencil(myID, serverIOPort, streamMeta)

function sendData() {
	stencil.sendToStream('A data packet from peer ' + myID)
}

for (var i = 0; i < numOfPackets; i++) {
	sendData()
}

