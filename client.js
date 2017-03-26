// var port = process.argv[2]

// var serverIO = require('socket.io')(port)
// var clientIO = require('socket.io-client')

// function getPeerURL(host, port) {
// 	return 'http://' + host + ':' + port
// }

// var options = {
// 	'timeout': 3000,
// 	'reconnectionAttempts': 5,
// 	'reconnectionDelay': 4000
// }

// var socket1 = clientIO.connect(getPeerURL('localhost', 8000), options)

// var seq = 0

// socket1.on('connect', function() {
// 	console.log('connects')
// 	console.log(socket1.id)
// })

// socket1.on('connect_timeout', function (data) {
// 	console.log('timeout')
// })

// socket1.on('reconnect_failed', function() {
//     console.log('reconnect_failed')
// })

// socket1.on('peerInfo', function(msg) {
// 	console.log(msg)
// })

// function send() {
// 	socket1.emit('peerInfo', {'seq': seq++})
// }

// for (var i = 0; i < 50; i++) {
// 	setTimeout(send, i * 2000)
// }




var io = require('socket.io-client');
var ss = require('socket.io-stream');
var fs = require('fs')

var socket = io.connect('http://localhost:8000');
var stream = ss.createStream();
var filename = '123123';

ss(socket).emit('profile-image', {name: filename});
fs.createReadStream(filename).pipe(stream);