var fs = require('fs')
var bases = require('bases')
var utils = require('./utils')
var mkdirp = require('mkdirp')
var Logger = require('./logger')

// A log file size limit is equal to this number of 'F'
const logFileSizeLimit = 4		

const typeFieldLen = 1 					// 1 character
const keyLenFieldLen = 2				// 2 characters
const metadataLenFieldLen = 3			// 3 characters
const valueLenFieldLen = 4 				// 4 characters
const dataType = 0 						// '0' represents a value contains actual data
const referenceType = 1 				// '1' represents a value contains a reference to actual data(used in storing large data)
const checksumAlgorithm = 'sha1'
const SHA1Len = 40

// Normally, the checkpoint position field length should be one bit longer than the checkpoint length field length 
const checkpointPositionFieldLen = 6
const checkpointLenFieldLen = 5

// Note: for now we simply make checkpoints periodically
// An optimization is to make checkpoints when necessary(program terminates), and 
// not to make checkpoints when the log system is not updated
const checkpointingInterval = 7000

var checkpointMaxLength
var checkpointMaxPosition


// Actually, these two paths are fixed and checkpointFolder is unnecessary,
// but here these two paths are set here based on id for testing purpose. 
var checkpointFilePath
var logFolder
var checkpointFolder = 'checkpoints/'


var Storage = function(id) {
	checkpointFilePath = checkpointFolder + id
	logFolder = 'log_system/' + id + '/'

	checkpointMaxLength = bases.fromBase16(this._generateFs(checkpointLenFieldLen))
	checkpointMaxPosition = bases.fromBase16(this._generateFs(checkpointPositionFieldLen))

	logFileMaxLength = bases.fromBase16(this._generateFs(logFileSizeLimit))

	this._logger = new Logger()

	this._indices = {}
	
	this._nextAppendStartPosition = {}

	// Recovery start position is used with checkpoints to rebuild memory indices after restarting the storage system.
	// There might be some cases where before this position, there are
	// still some uncompleted log append tasks(appendLog is async), but we rebuild memory indices from this position.
	// Therefore, the log might have 'holes'. 
	this._recoveryStartPosition = {}

	this._init()

	this._makeCheckpointsPeriodically()
}

Storage.prototype._generateFs = function(FLength) {
	var Fs = ''
	for (var i = 0; i < FLength; i++) {
		Fs += 'f'
	}
	return Fs
}

// Version control module maintains version structure
// In the test, all keys are different, so the index is always 0 for now.
Storage.prototype.get = function(key, index, callback) {
	var self = this

	var versions = this._indices[key]['versions']

	if (versions == undefined) {
		callback('No such data', null, null)
	} else {
		var oneVersion = versions[index]

		var filePath = logFolder + oneVersion.file
		fs.open(filePath, 'r', function(err, fd) {
			if (err != null) callback(err, null, null)

			self._readAsync(fd, oneVersion.position, oneVersion.length, false, function(err, bytesRead, data) {
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

Storage.prototype._convertToLogFormat = function(value, maxLength) {
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

Storage.prototype.put = function(key, value, metadata, callback) {
	var self = this

	function appendToLog(key, value, metadata, callback) {
		var logRecord = ''

		//Stringify metadata to append to logs
		var stringifiedMetadata = JSON.stringify(metadata)

		var keyLen = self._convertToLogFormat(key.length, keyLenFieldLen)
		var metadataLen = self._convertToLogFormat(stringifiedMetadata.length, metadataLenFieldLen)
		var valueLen = self._convertToLogFormat(value.length, valueLenFieldLen)
		
		var withoutChecksum = dataType + keyLen + metadataLen + valueLen + key + stringifiedMetadata + value
		var checksum = utils.createHash(withoutChecksum, checksumAlgorithm)

		logRecord = checksum + withoutChecksum
		var logRecordLength = logRecord.length

		var logFile
		var logRecordPostion
		var filePath

		var nextAppendStartPosition = self._nextAppendStartPosition.position + logRecordLength

		// Note we don't consider storing a very large record which could be even larger than the log file max length
		// If we want to store such a large record, we have to use reference data type
		if (nextAppendStartPosition > logFileMaxLength) {
			logFile = (parseInt(self._nextAppendStartPosition.file) + 1) + ''
			filePath = logFolder + logFile

			logRecordPostion = 0

			//Update next append start position and file
			self._nextAppendStartPosition.position = logRecordLength
			self._nextAppendStartPosition.file = logFile

		} else {
			logFile = self._nextAppendStartPosition.file
			filePath = logFolder + logFile

			logRecordPostion = self._nextAppendStartPosition.position

			//Only update next append start position
			self._nextAppendStartPosition.position += logRecordLength
		}

		fs.appendFile(filePath, logRecord, function(err) {
  			if (err != null) throw err
  			callback(err, logFile, logRecordPostion, logRecordLength)
		})
	}

	function updateRecoveryStartPosition(file, logRecordPostion, logRecordLength) {
		var position = logRecordPostion + logRecordLength
		// Note: a late but successsful old file append might also change the recovery start point
		// Solve this when considering multiple log files!!!!!!!!!!!!!!!!
		if (file != self._recoveryStartPosition.file || self._recoveryStartPosition.position < position) {
			self._recoveryStartPosition.file = file
			self._recoveryStartPosition.position = position
		}
	}

	appendToLog(key, value, metadata, function(err, file, logRecordPostion, logRecordLength){
		if (err != null) throw err
		self._updateIndices(key, metadata, file, logRecordPostion, logRecordLength)
		updateRecoveryStartPosition(file, logRecordPostion, logRecordLength)
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

Storage.prototype._updateIndices = function(key, metadata, file, logRecordPostion, logRecordLength) {
	if (this._indices[key] == undefined) {
		this._indices[key] = {}
		this._indices[key]['versions'] = []
	}

	var index = {}
	index.file = file
	index.position = logRecordPostion
	index.length = logRecordLength
	index.metadata = metadata

	this._indices[key]['versions'].push(index)

}

Storage.prototype._writeSync = function(fd, position, data) {
	var dataLength = data.length
	var buffer = Buffer.from(data)
	fs.writeSync(fd, buffer, 0, dataLength, position)
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

Storage.prototype._updateCheckpointFileHeaderSync = function(fd, position, length) {
	var checkpointPosition = this._convertToLogFormat(position, checkpointPositionFieldLen)
	var checkpointLength = this._convertToLogFormat(length, checkpointLenFieldLen)
	var header = checkpointPosition + checkpointLength
	this._writeSync(fd, 0, checkpointPosition + checkpointLength)
}

Storage.prototype._init = function() {
	var self = this

	function comparator(v1, v2) {
		if (v1 < v2) return -1
		if (v1 > v2) return 1
		return 0	
	}

	function convertToInt(arr) {
		for (var i in arr) {
			arr[i] = parseInt(arr[i])
		}
		return arr
	}

	//Note: not consider reference data type for now
	function restoreMemoryIndicesFromOneLog(file, restoreStartPosition) {
		var logRecordStartPostion = restoreStartPosition
		var filePath = logFolder + file
		var fd = fs.openSync(filePath, 'r')
		var headerLength = SHA1Len + typeFieldLen + keyLenFieldLen + metadataLenFieldLen + valueLenFieldLen

		while (true) {
			var header = self._readSync(fd, logRecordStartPostion, headerLength, false)
			var checksum = header.substr(0, SHA1Len)
			var keyLen = bases.fromBase16(header.substr(SHA1Len + typeFieldLen, keyLenFieldLen))
			var metadataLen = bases.fromBase16(header.substr(SHA1Len + typeFieldLen + keyLenFieldLen, metadataLenFieldLen))
			var valueLen = bases.fromBase16(header.substr(SHA1Len + typeFieldLen + keyLenFieldLen + metadataLenFieldLen, valueLenFieldLen))
			var dataLen = keyLen + metadataLen + valueLen
			try {
				var data = self._readSync(fd, logRecordStartPostion + headerLength, dataLen, false)
			} catch (err) {
				if (err == 'Offset is out of bounds') {
					return logRecordStartPostion
				}
			}
			var logRecord = header + data
			if (self._verifyChecksum(logRecord)) {
				var key = data.substr(0, keyLen)
				var metadata = JSON.parse(data.substr(keyLen, metadataLen))
				var logRecordLength = headerLength + dataLen
				self._updateIndices(key, metadata, file, logRecordStartPostion, logRecordLength)

				logRecordStartPostion += logRecordLength
			} else {
				return logRecordStartPostion
			}
		}
	}

	function loadCheckpointInfo() {
		var exists = fs.existsSync(checkpointFilePath)
		
		if (exists) {
			var checkpointFileDescriptor = fs.openSync(checkpointFilePath, 'r')
			var checkpointPostionAndLengthRawData = self._readSync(checkpointFileDescriptor, 0, checkpointPositionFieldLen + checkpointLenFieldLen, false)
			var checkpointPosition = bases.fromBase16(checkpointPostionAndLengthRawData.substr(0, checkpointPositionFieldLen))
			var checkpointLength = bases.fromBase16(checkpointPostionAndLengthRawData.substr(checkpointPositionFieldLen, checkpointLenFieldLen))

			if (checkpointLength != 0) {
				var checkpointRawData = self._readSync(checkpointFileDescriptor, checkpointPosition, checkpointLength, false)
				if (!self._verifyChecksum(checkpointRawData)) {
					return
				}
				var checkpoint = JSON.parse(checkpointRawData.substr(SHA1Len))
				self._indices = checkpoint.indices
				self._recoveryStartPosition = checkpoint.recoveryStartPosition

				self._logger.logOneMessage('After reading the latest checkpoint, memory indices:')
				self._logger.logOneMessage(JSON.stringify(self._indices, null, 4))
				self._logger.logOneMessage('After reading the latest checkpoint, the size of memory indices: ' + Object.keys(self._indices).length)
				self._logger.logOneMessage('')
				self._logger.logOneMessage('')
			}
		} else {
			var checkpointFileDescriptor = fs.openSync(checkpointFilePath, 'a+')
			self._updateCheckpointFileHeaderSync(checkpointFileDescriptor, checkpointPositionFieldLen + checkpointLenFieldLen, 0)
		}
	}

	function restoreMemoryIndices() {
		var logs = convertToInt(fs.readdirSync(logFolder))

		//Note file names here are all integers 
		if (logs.length != 0) {
			var recoveryStartFile = -1
			var recoveryStartPosition = 0
			var startPosition
			var nextAppendStartPosition = {}

			if (self._recoveryStartPosition.file != undefined) {
				recoveryStartFile = self._recoveryStartPosition.file
				recoveryStartPosition = self._recoveryStartPosition.position
			}

			logs.sort(comparator)

			for (var i in logs) {
				if (recoveryStartFile > logs[i]) {
					continue
				}

				if (recoveryStartFile == logs[i]) {
					startPosition = recoveryStartPosition
				}

				if (recoveryStartFile < logs[i]) {
					startPosition = 0
				}

				nextAppendStartPosition.file = logs[i]
				nextAppendStartPosition.position = restoreMemoryIndicesFromOneLog(logs[i], startPosition)
			}

			self._nextAppendStartPosition = nextAppendStartPosition

		} else {
			var firstLogName = '0'
			self._nextAppendStartPosition.file = firstLogName
			self._nextAppendStartPosition.position = 0
		}
	}

	this._logger.recoveryStart()

	// Create folders if they don't exist
	mkdirp.sync(logFolder)
	mkdirp.sync(checkpointFolder)

	// If there is a checkpoint file, then load checkpoint info from the checkpoint file
	// Otherwise, create one file and initialize this file
	loadCheckpointInfo()

	// Rebuild memory indices by checkpoint info and reading log records
	restoreMemoryIndices()
	
	this._logger.logOneMessage('After recovery process, memory indices:')
	this._logger.logOneMessage(JSON.stringify(this._indices, null, 4))
	this._logger.logOneMessage('After recovery, the size of memory indices: ' + Object.keys(this._indices).length)
	this._logger.logElapsedTime()
	this._logger.recoveryEnd()
}

// Append to file and when file is too large, wrap over.
// First append synchronously -> update metadata at the front of the file
// If append is unsuccessful, we can still use the previous checkpoint 
Storage.prototype._makeCheckpoint = function() {
	var checkpointObj = {}
	checkpointObj.indices = this._indices
	checkpointObj.recoveryStartPosition = this._recoveryStartPosition
	checkpointObj.timestamp = new Date()

	var stringifiedCheckpoint = JSON.stringify(checkpointObj)
	var checksum = utils.createHash(stringifiedCheckpoint, checksumAlgorithm)
	var checkpointLength = SHA1Len + stringifiedCheckpoint.length
	var checkpoint = checksum + stringifiedCheckpoint

	if (checkpointLength > checkpointMaxLength) {
		console.log('The checkpoint is too long!!')
		return
	}

	var checkpointFileDescriptor = fs.openSync(checkpointFilePath, 'r+')
	var lastCheckpointMeta = this._readSync(checkpointFileDescriptor, 0, checkpointPositionFieldLen + checkpointLenFieldLen, false)
	var lastCheckpointPosition = bases.fromBase16(lastCheckpointMeta.substr(0, checkpointPositionFieldLen))
	var lastCheckpointLength = bases.fromBase16(lastCheckpointMeta.substr(checkpointPositionFieldLen, checkpointLenFieldLen))

	var checkpointStartPosition = lastCheckpointPosition + lastCheckpointLength
	// Check whether we need to wrap the write
	if (checkpointStartPosition > checkpointMaxPosition) {
		console.log('Checkpointing wraps around to the beginning of the checkpoint file!!')
		checkpointStartPosition = checkpointPositionFieldLen + checkpointLenFieldLen
	}
	this._writeSync(checkpointFileDescriptor, checkpointStartPosition, checkpoint)

	this._updateCheckpointFileHeaderSync(checkpointFileDescriptor, checkpointStartPosition, checkpointLength)
}

Storage.prototype._makeCheckpointsPeriodically = function() {
	setInterval(this._makeCheckpoint.bind(this), checkpointingInterval)
}

module.exports = Storage