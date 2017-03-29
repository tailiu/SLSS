var fs = require('fs')

var Storage = function() {
	this._availablePackets = []
}

Storage.prototype.getData = function(position) {
	return this._availablePackets[position]
}

Storage.prototype.findPositionInAvailablePacketList = function(packetMsg) {
	var availablePackets = this._availablePackets

	for (var i = 0; i < availablePackets.length; i++) {
		if (availablePackets[i].header.source == packetMsg.source && availablePackets[i].header.sequenceNumber == packetMsg.sequenceNumber) {
			return i
		}
	}
	return -1
}

Storage.prototype.putData = function(data) {
	this._availablePackets.push(data)
}

Storage.prototype.getAllData = function() {
	return this._availablePackets
}

Storage.prototype.getAllMetadata = function() {
	var metadata = []

	for (var i in this._availablePackets) {
		metadata.push(this._availablePackets[i].header)
	}

	return metadata
}

module.exports = Storage