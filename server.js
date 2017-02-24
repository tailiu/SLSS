// var port = process.argv[2]

var serverIO = require('socket.io')(8000)
var clientIO = require('socket.io-client')


serverIO.on('connection', function (serverSocket) {
	console.log('connection')

	serverSocket.on('peerInfo', function(msg) {
		console.log(msg)
	})
})

// var nock = require('nock');

// var couchdb = nock('http://localhost:8000')
//                 .get('/')
//                 .socketDelay(10000) // 2 seconds
//   				.reply(200, '<html></html>')