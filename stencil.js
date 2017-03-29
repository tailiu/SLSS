var Transport = require('./transport')
var stream = require('./stream')
var Logger = require('./logger')
var Storage = require('./storage')
var events = require('events')
var inherits = require('util').inherits

const logInterval = 5000

var Stencil = function(id, port) {
	events.call(this)

	this._id = id
	this._port = port
	this._streamMeta = stream.createStreamMeta()

	this._sequenceNumber = 0
	
	this._storage = new Storage()
	this._transport = new Transport(this._id, this._port, this._streamMeta)
	this._logger = new Logger()

	this._bindTransportEventHandlers()
	
	this._writeToLogPeriodically()
}

inherits(Stencil, events)



Stencil.prototype.sendToStream = function(data) {
	var metadata = this._createMetadata()
	var packet = {
		'header': metadata,
		'data': data
	}
	this._storage.putData(packet)
	this._transport.sendToStream(packet)
}

//Add communication overlay metadata
Stencil.prototype._createMetadata = function() {
	var metadata = {
		'timestamp': new Date(),
		'source': this._id,
		'destination': 'all',
		'sequenceNumber': this._sequenceNumber++
	}

	return metadata
}



Stencil.prototype._bindTransportEventHandlers = function() {
	this._transport.on('findPositionInStorageSystem', this._findPosition.bind(this))
	this._transport.on('getDataFromStorageSystem', this._getData.bind(this))
	this._transport.on('putDataToStorageSystem', this._putData.bind(this))
	this._transport.on('getAllDataFromStorageSystem', this._getAllData.bind(this))
	this._transport.on('getAllMetadataFromStorageSystem', this._getAllMetadata.bind(this))
}

Stencil.prototype._findPosition = function(msg, callback) {
	callback(this._storage.findPositionInAvailablePacketList(msg))
}

Stencil.prototype._getData = function(msg, callback) {
	var self = this

	self._findPosition(msg, function(position){
		callback(self._storage.getData(position))
	})
}

Stencil.prototype._putData = function(data, callback) {
	this._storage.putData(data)
	callback()
}

Stencil.prototype._getAllData = function(callback) {
	callback(this._storage.getAllData())
}

Stencil.prototype._getAllMetadata = function(callback) {
	callback(this._storage.getAllMetadata())
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
	this._logger.logMultipleMessages(['Available Packets:', this._storage.getAllData()])

	this._logger.logOneMessage('Length of Available Packets: ' + this._storage.getAllData().length)

	this._logger.logElapsedTime()

	this._logger.oneLogEnd()
}

Stencil.prototype._writeToLogPeriodically = function() {
	setInterval(this._writeToLog.bind(this), logInterval)
}

module.exports = Stencil