var port = process.argv[2]

var serverIO = require('socket.io')(port)
var clientIO = require('socket.io-client')

function getPeerURL(host, port) {
	return 'http://' + host + ':' + port
}

var options = {
	'timeout': 3000
}

var socket1 = clientIO.connect(getPeerURL('localhost', 8000), options)

socket1.on('connect', function() {
	console.log('connects')
	console.log(socket1.id)
    socket1.emit('peerInfo', {'host': 'localhost', 'port': port})
})

socket1.on('connect_timeout', function (data) {
	console.log('timeout')
})


