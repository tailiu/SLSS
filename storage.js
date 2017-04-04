var fs = require('fs')
var bases = require('bases')
var utils = require('./utils')

const logFolder = 'log_system'
const logFileSizeLimit = 10485760		// log file size limit is 10MB
const keyLenFieldLen = 2				// 2 characters
const metadataLenFieldLen = 3			// 3 characters
const valueLenFieldLen = 4 				// 4 characters
const typeFieldLen = 1 					// 1 character
const dataType = 0 						// '0' represents a value contains actual data
const referenceType = 1 				// '1' represents a value contains a reference to actual data(used in storing large data)
const checksumAlgorithm = 'sha1'
const SHA1Len = 40

const checkPointFile = 'checkpoints'
const checkPointPositionDigitLen = 5
const checkPointLenDigitLen = 4

//1. Checkpoints
//2. Multiple log files
var Storage = function() {
	this._indices = {}
	
	this._nextAppendStartPosition = {}

	// Recovery start position is used with checkpoints to rebuild memory indices after restarting the storage system.
	// There might be some cases where before this position, there are
	// still some uncompleted log append tasks(appendLog is async), but we rebuild memory indices from this position.
	// Therefore, the log might have 'holes'. 
	this._recoveryStartPosition = {}

	this._logs = []

	this._init()
}

// Sequence numbers would be replaced by index to the version array in the future
// Version control module maintains version structure
// In the test, all keys are different, so the index is always 0 for now.
Storage.prototype.get = function(key, index, callback) {
	var self = this

	var versions = this._indices[key]['versions']

	if (versions == undefined) {
		callback('No such data', null, null)
	} else {
		var oneVersion = versions[index]

		this._readAsync(oneVersion.fd, oneVersion.position, oneVersion.length, false, function(err, bytesRead, data) {
			if (err != null) callback(err, null, null)

			if (!self._verifyChecksum(data)) {
				callback('Data fails checksum verification', null, null)
			} else {
				// Key length
				var keyLen = key.length

				// Get metadata length 
				var metadataLenFieldStartPosition = SHA1Len + typeFieldLen + keyLenFieldLen
				var metadataLenFieldEndPosition = metadataLenFieldStartPosition + metadataLenFieldLen
				var metadataLen = bases.fromBase16(data.substring(metadataLenFieldStartPosition, metadataLenFieldEndPosition))

				// Get metadata
				var metadataStartPosition = SHA1Len + typeFieldLen + keyLenFieldLen + metadataLenFieldLen + valueLenFieldLen + keyLen
				var metadataEndPosition = metadataStartPosition + metadataLen
				var metadata = data.substring(metadataStartPosition, metadataEndPosition)

				// Get value length
				var valueLenFieldStartPosition = metadataLenFieldEndPosition
				var valueLenFieldEndPosition = valueLenFieldStartPosition + valueLenFieldLen
				var valueLen = bases.fromBase16(data.substring(valueLenFieldStartPosition, valueLenFieldEndPosition))

				// Get Value
				var valueStartPosition = metadataEndPosition
				var valueEndPosition = valueStartPosition + valueLen
				var value = data.substring(valueStartPosition, valueEndPosition)

				callback(null, metadata, value)	
			}
		})
	}
}

Storage.prototype._verifyChecksum = function(data) {
	var checksum = data.substring(0, SHA1Len)
	var dataTobeVerified = data.substring(SHA1Len)
	return utils.verifyHash(checksum, dataTobeVerified, checksumAlgorithm)
}

Storage.prototype._readAsync = function(fd, position, length, safe, callback) {
	var buffer 
	if (safe) {
		buffer = Buffer.alloc(length)
	} else {
		buffer = Buffer.allocUnsafe(length)
	}

	fs.read(fd, buffer, 0, length, position, function(err, bytesRead, buffer) {
		if (err != null) throw err

		callback(err, bytesRead, buffer.toString())
	})
}

Storage.prototype._readSync = function(fd, position, length, safe) {
	var buffer 
	if (safe) {
		buffer = Buffer.alloc(length)
	} else {
		buffer = Buffer.allocUnsafe(length)
	}

	fs.readSync(fd, buffer, 0, length, position)
	return buffer.toString()
}

Storage.prototype.put = function(key, value, metadata, callback) {
	var self = this

	function convertToLogFormat(value, maxLength) {
		var base16Value = bases.toBase16(value)

		if (base16Value.length > maxLength) {
			console.log('Data is too long')
			return 
		}

		var lengthDiff = maxLength - base16Value.length
		for (var i = 0; i < lengthDiff; i++) {
			base16Value = '0' +  base16Value
		}

		return base16Value
	}

	function appendToLog(key, value, metadata, callback) {
		var logRecord = ''

		//Stringify metadata to append to logs
		var stringifiedMetadata = JSON.stringify(metadata)

		var keyLen = convertToLogFormat(key.length, keyLenFieldLen)
		var metadataLen = convertToLogFormat(stringifiedMetadata.length, metadataLenFieldLen)
		var valueLen = convertToLogFormat(value.length, valueLenFieldLen)
		
		var withoutChecksum = dataType + keyLen + metadataLen + valueLen + key + stringifiedMetadata + value
		var checksum = utils.createHash(withoutChecksum, checksumAlgorithm)

		logRecord = checksum + withoutChecksum

		if (self._nextAppendStartPosition.fd == undefined) {
			var fileName = utils.createRandom('sha1')
			var logFileDescriptor = fs.openSync(logFolder + '/' + fileName, 'a+')
			self._nextAppendStartPosition.fd = logFileDescriptor
			self._nextAppendStartPosition.position = 0
		}

		var fd = self._nextAppendStartPosition.fd
		var logRecordPostion = self._nextAppendStartPosition.position
		var logRecordLength = logRecord.length

		//Update next append start position
		self._nextAppendStartPosition.position += logRecordLength

		fs.appendFile(fd, logRecord, function(err) {
  			if (err != null) throw err
  			callback(err, fd, logRecordPostion, logRecordLength)
		})
	}

	function updateIndices(key, metadata, fd, logRecordPostion, logRecordLength) {
		if (self._indices[key] == undefined) {
			self._indices[key] = {}
			self._indices[key]['versions'] = []
		} 

		var index = {}
		index.fd = fd
		index.position = logRecordPostion
		index.length = logRecordLength
		index.metadata = metadata

		self._indices[key]['versions'].push(index)

	}

	function updateLatestCompletionPosition(fd, logRecordPostion, logRecordLength) {
		var position = logRecordPostion + logRecordLength
		// Note: a late but successsful old file append might also change the recovery start point
		// Solve this when considering multiple log files!!!!!!!!!!!!!!!!
		if (fd != self._recoveryStartPosition.fd || self._recoveryStartPosition.position < position) {
			self._recoveryStartPosition.fd = fd
			self._recoveryStartPosition.position = position
		}
	}

	appendToLog(key, value, metadata, function(err, fd, logRecordPostion, logRecordLength){
		if (err != null) throw err
		updateIndices(key, metadata, fd, logRecordPostion, logRecordLength)
		updateLatestCompletionPosition(fd, logRecordPostion, logRecordLength)
		callback()
	})

}

//key, and version vector(sequence number for now, 
//but version vector should be generated by version module? yes
//latest version vector cached by version module or storage module? 
//cache and maintain latest version pointer and version structures in version module.)
//determine a value
//How would it influence storage interfaces, and stencil interfaces?
Storage.prototype.getMetadata = function(key, index) {
	if (key == undefined) {
		return this._indices
	}
	if (index == undefined) {
		return this._indices[key]
	}
	if (this._indices[key] == undefined || this._indices[key].versions[index] == undefined) {
		return undefined
	}
	return this._indices[key].versions[index].metadata
}


Storage.prototype._init = function() {
	// var checkPointFileDescriptor = fs.openSync(checkPointFile, 'a+')
	// var checkPointPostion = bases.fromBase16(this._readSync(checkPointFileDescriptor, 0, checkPointPositionDigitLen, true))
	// var checkPointLength = bases.fromBase16(this._readSync(checkPointFileDescriptor, 0, checkPointLenDigitLen, true))
	
	// if (checkPointLength == 0) {
	// }
	// this._logs = fs.readdirSync(logFolder)
}

module.exports = Storage