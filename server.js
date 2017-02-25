// var port = process.argv[2]

var serverIO = require('socket.io')(8000)
var clientIO = require('socket.io-client')

var seq = 0

var time = 0
serverIO.on('connection', function (serverSocket) {
	console.log('connection')
	console.log(serverSocket.id)

	serverSocket.on('peerInfo', function(msg) {
		console.log(msg)
	})

	// if (time == 0) {
	// 	for (var i = 0; i < 20; i++) {
	// 		setTimeout(function(){
	// 			serverSocket.emit('peerInfo', {'seq': seq++})
	// 		}, i * 2000)
	// 	}
	// }

	// time++

	serverSocket.on('disconnect', function(msg) {
		console.log('disconnect')
	})
})

var _ = require('underscore')

var arr = ['1', '2', '3']
var index = _.indexOf(arr, '2' + '')
console.log(index)

console.log(('9000' == 9000))
