var Transport = require('./transport')
var stream = require('./stream')
var Logger = require('./logger')
var Storage = require('./storage')
var utils = require('./utils')

const logInterval = 5000

var Stencil = function(id, port) {
	this._id = id
	this._port = port
	this._streamMeta = stream.createStreamMeta()
	
	this._storage = new Storage(this._id)
	this._transport = new Transport(this._id, this._port, this._streamMeta)
	this._logger = new Logger()

	this._bindTransportEventHandlers()
	
	this._writeToLogPeriodically()
}



Stencil.prototype.sendToStream = function(key, value) {
	var self = this

	//Add communication overlay metadata
	function createMetadata() {
		var metadata = {
			'timestamp': new Date(),
			'source': self._id,
			'destination': 'all'
		}

		return metadata
	}

	var metadata = createMetadata()
	var packet = {
		'header': {
			'key': key,
			'metadata': metadata
		},
		'data': value
	}
	this._storage.put(key, value, metadata, function(){
		self._transport.sendToStream(packet)
	})
}



Stencil.prototype._bindTransportEventHandlers = function() {
	this._transport.on('checkDataExistsInStorageSystem', this._checkDataExists.bind(this))
	this._transport.on('getDataFromStorageSystem', this._getData.bind(this))
	this._transport.on('storeRequestedData', this._storeRequestedData.bind(this))
	this._transport.on('getAllMetadataFromStorageSystem', this._getAllMetadata.bind(this))
}

Stencil.prototype._checkDataExists = function(msg, callback) {
	if (this._storage.getMetadata(msg.key, 0) == undefined) {
		callback(false)
	} else {
		callback(true)
	}
}

Stencil.prototype._getData = function(msg, callback) {
	this._storage.get(msg.key, 0, function(err, metadata, value){
		if (err != null) {
			callback(err)
		} else {
			callback(value)
		}
	})
}

Stencil.prototype._storeRequestedData = function(msg, data, callback) {
	this._storage.put(msg.key, data, msg.metadata, function(){
		callback()
	})
}

Stencil.prototype._getAllMetadata = function(callback) {

	//Format all metadata to the communication overlay packet header
	function formatAllMetadataToPacketHeader(allMetadata) {
		var formatedMetadata = []

		for (var key in allMetadata) {
			for (var i in allMetadata[key].versions) {
				var oneMetadata = {}
				oneMetadata.key = key
				oneMetadata.metadata = allMetadata[key].versions[i].metadata
				formatedMetadata.push(oneMetadata)
			}
		}

		return formatedMetadata
	}

	var allMetadata = this._storage.getMetadata(undefined, undefined)
	var allFormatedMetadata = formatAllMetadataToPacketHeader(allMetadata)
	callback(allFormatedMetadata)
}




Stencil.prototype._writeToLog = function() {
	this._logger.oneLogStart()

	this._logger.logOneMessage('Me: ' + this._id)

	this._logger.logOneMessage('My neighbours:')
	for (var i in this._transport._neighbours) {
		this._logger.logOneMessage(this._transport._neighbours[i].peerID)
	}

	this._logger.logMultipleMessages(['Outstanding Packets:', this._transport._outstandingPackets])
	this._logger.logMultipleMessages(['Desired Packets:', this._transport._desiredPackets])
	this._logger.logMultipleMessages(['Memory Indices:', JSON.stringify(this._storage._indices, null, 4)])

	this._logger.logOneMessage('The Size of My Storage(Memory Indices): ' + Object.keys(this._storage._indices).length)

	this._logger.logElapsedTime()

	this._logger.oneLogEnd()
}

Stencil.prototype._writeToLogPeriodically = function() {
	setInterval(this._writeToLog.bind(this), logInterval)
}

module.exports = Stencil