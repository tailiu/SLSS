var Stencil = require('../stencil')

const serverIOPort = process.argv[2]


// function createHash(dataToHash) {
// 	var hash = crypto.createHash('sha256')
// 	hash.update(dataToHash)
// 	return hash.digest('hex')
// }

// function createRandom() {
// 	var current_date = (new Date()).valueOf().toString()
// 	var random = Math.random().toString()
// 	return createHash(current_date + random)
// }

//For now for testing purpose, we only use each unique port number as
//peer ID, but we can replace the port number with a random number generated
//by createRandom() in the future
function createMyID() {
	return serverIOPort + ''
}

var myID = createMyID()



var stencil = new Stencil(myID, serverIOPort)

function sendData(number) {
	stencil.sendToStream('|' + myID + ':' + number + '|',  'A data packet from peer ' + myID + ' with key ' + '|' + myID + ':' + number + '|')
}



const numOfPackets = 10

for (var i = 0; i < numOfPackets; i++) {
	sendData(i)
}