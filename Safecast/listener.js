var http = require('http');
function handle(req,res){
	    if(req.method=='POST'){
		            handlePost(req,res);
			        }
	        else{
			        res.writeHead(200, {'Content-Type': 'text/plain'});
				        res.end();
					    }
}
function handlePost(req, res){
	    req.on('data', function(chunk) {
		                      console.log("Receive_Event::" + unescape(chunk.toString()));
				                       });
	        res.writeHead(200, {'Content-Type': 'text/json'});
		    res.end("{\"status\":\"success\"}");
}
var server= http.createServer(handle);
server.listen(1337, "192.168.1.101");
console.log('Server running at http://192.168.1.101:1337/');
