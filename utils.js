//Notice: the indexArr should contain index in the ascending order
exports.removeElementsFromArr = function(indexArr, arr) {
	for (var i = indexArr.length - 1; i >= 0; i--) {
		arr.splice(indexArr[i], 1)
	}
}

exports.findIndexInPacketList = function(packets, packetMsg) {
	for (var i = 0; i < packets.length; i++) {
		if (packets[i].source == packetMsg.source && packets[i].sequenceNumber == packetMsg.sequenceNumber) {
			return i
		}
	}
	return -1
}

exports.deepCloneObject = function(object) {
	return JSON.parse(JSON.stringify(object))
}