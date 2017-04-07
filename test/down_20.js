var childProcess = require('child_process')

const numOfPeers = 20
const basePort = 10000
const difference = 100

var ports = []

for (var i = 0; i < numOfPeers; i++) {
	ports.push(basePort + i * difference)
}

for (var i in ports) {
	var output = childProcess.execSync("netstat -nlp 2>/dev/null | grep :" + ports[i] + " | awk '{print $7}'").toString()
	var arr = output.split('\n')
	var port = arr[0].split('/')[0]
	
	childProcess.execSync('kill ' + port)
}