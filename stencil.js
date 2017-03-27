var Transport = require('./transport')
var stream = require('./stream')
var Logger = require('./logger')

const logInterval = 5000

var Stencil = function(id, port) {
	this._id = id
	this._port = port
	this._streamMeta = stream.createStreamMeta()

	this._transport = new Transport(this._id, this._port, this._streamMeta)
	this._logger = new Logger()

	this._writeToLogPeriodically()
}

Stencil.prototype.sendToStream = function(data) {
	this._transport.sendToStream(data)
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
	this._logger.logMultipleMessages(['Available Packets:', this._transport._availablePackets])

	this._logger.logOneMessage('Length of Available Packets: ' + this._transport._availablePackets.length)

	this._logger.logElapsedTime()

	this._logger.oneLogEnd()
}

Stencil.prototype._writeToLogPeriodically = function() {
	setInterval(this._writeToLog.bind(this), logInterval)
}

module.exports = Stencil