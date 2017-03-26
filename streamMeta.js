var kad = require('kad')

const DHTPort = 8300
const DHTAddr = 'localhost'
const DHTdb = 'db1'
const DHTSeed = {
	address: '127.0.0.1',
	port: 8200
}

var DHTNode = new kad.Node({
	transport: kad.transports.UDP(kad.contacts.AddressPortContact({
		address: DHTAddr,
		port: DHTPort
	})),
	storage: kad.storage.FS(DHTdb)
})

DHTNode.connect(DHTSeed, function(err) {
	var key = 'test'
	var value = {
		'memberList': [
			{'host': 'localhost', 'port': 5000},
			{'host': 'localhost', 'port': 6000}, 
			{'host': 'localhost', 'port': 7000},
			{'host': 'localhost', 'port': 8000}
		]
	}
	DHTNode.put(key, value, function() {
		console.log('Put done')
	})
})





const DHTPort = 9000
const DHTAddr = 'localhost'
const DHTdb = 'db2'
const DHTSeed = {
	address: '127.0.0.1',
	port: 8200
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
