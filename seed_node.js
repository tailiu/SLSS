var kad = require('kad')

const DHTSeed = {
 	host: '127.0.0.1',
	port: 8200
}

const db = 'db0'

new kad.Node({
	transport: kad.transports.UDP(kad.contacts.AddressPortContact({
		address: DHTSeed.host,
		port: DHTSeed.port
	})),
	storage: kad.storage.FS(db)
})