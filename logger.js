var Logger = function () {
	this.startTime = new Date()
}

Logger.prototype.oneLogStart = function() {
	console.log('****************** One Log Start *****************************')
}

Logger.prototype.logOneMessage = function(message) {
	console.log(message)
}

Logger.prototype.logMultipleMessages = function(messages) {
	for (var i in messages) {
		console.log(messages[i])
	}
}

Logger.prototype.logElapsedTime = function() {
	var now = new Date()
	var elapsedTime = now - this.startTime
	console.log('Elapsed Time: ' + elapsedTime + ' ms')
}

Logger.prototype.oneLogEnd = function() {
	console.log('******************** One Log End ***************************')
}

module.exports = Logger