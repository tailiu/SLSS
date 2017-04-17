var crypto = require('crypto')

function createHash(dataToHash, algorithm) {
	var hash = crypto.createHash(algorithm)
	hash.update(dataToHash)
	return hash.digest('hex')
}

// Note: the indexArr should contain index in the ascending order
exports.removeElementsFromArr = function(indexArr, arr) {
	for (var i = indexArr.length - 1; i >= 0; i--) {
		arr.splice(indexArr[i], 1)
	}
}

// Note: assuming that keys are unique, for now we get index only by comparing key  
exports.findIndexInPacketList = function(packets, packetMsg) {
	for (var i = 0; i < packets.length; i++) {
		if (packets[i].key == packetMsg.key) {
			return i
		}
	}
	return -1
}

exports.deepCloneObject = function(object) {
	return JSON.parse(JSON.stringify(object))
}

exports.createHash = function(dataToHash, algorithm) {
	return createHash(dataToHash, algorithm)
}

exports.createRandom = function(algorithm) {
	var current_date = (new Date()).valueOf().toString()
	var random = Math.random().toString()
	return createHash(current_date + random, algorithm)
}

exports.verifyHash = function(hash, data, algorithm) {
	if (createHash(data, algorithm) === hash) {
		return true
	} else {
		return false
	}
}