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