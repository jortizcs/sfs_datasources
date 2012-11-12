require('./sfsconnector.js');
var http = require('http');


//default
var sfshost="jortiz81.homelinux.com";
var sfsport=8080;
var safecast_root="/safecast";

var cache=new Object();

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
    req.on('data', 
            function(chunk) {
                //raw data starts with 'data=', so remove it
                var raw = unescape(chunk.toString()).substring(5,unescape(chunk.toString()).length);
                try {
                    var inputData = JSON.parse(raw);
                    //console.log("Receive_Event::" +JSON.stringify(inputData));
                    var datapt = new Object();
                    datapt.timestamp = new Date(inputData.timestamp).getTime()/1000;
                    datapt.value=inputData.triggering_datastream.value.value;
                    //console.log('[date=' + date + ',unixts=' + date.getTime()/1000 + ']');
                    //console.log(JSON.stringify(datapt));
                    var dataptstr = JSON.stringify(datapt);

                    //key = trigger_id::feed_id:stream_id
                    var key = inputData.id + "::" + inputData.environment.id + ":" +
                            inputData.triggering_datastream.id;
                    console.log("Looking up cache." + key);
                    if(typeof(cache.key) != "undefined"){
                        var ppath = cache.key;
                        console.log(dataptstr + "->" + ppath);
                    }
                } catch(e){
                    console.log("Error!  " + e);
                }
            }
    );
    res.writeHead(200, {'Content-Type': 'text/json'});
    res.end("{\"status\":\"success\"}");
}

function loadConfigFile(){
	var fs = require('fs');
    fs.readFile('config.json', 'ascii', 
        function(err, data){
            if(err){
                console.error("could not open file %s, error: %s\n", 'config.json',err);
                process.exit(1);
            }
            var configObj = JSON.parse(data);
            //console.log(JSON.stringify(configObj));
            if(typeof(configObj.sfs_host) != "undefined")
                sfshost = configObj.sfs_host;
            console.log("sfs_host=" + sfshost);
            if(typeof(configObj.sfs_port) != "undefined")
                sfsport = configObj.sfs_port;
            console.log("sfs_port=" + sfsport);
            if(typeof(configObj.safecast_root) != "undefined")
                safecast_root = configObj.safecast_root;
            console.log("safecast_root=" + safecast_root);
        }
    );
}

function setupEnvironment(){
    loadConfigFile();
    var fs = require('fs');
    fs.readFile('safecast_depinfo2.json', 'ascii',
        function(err, data){
            if(err){
                console.error("Could not open file %s, error=%s\n", 
                    'safecastinfo2.json', err);
                process.exit(1);
            }
            var devinfoObj = JSON.parse(data);
            var feeds = devinfoObj.feeds;
            if(safecast_root.substr(safecast_root.length-1)=='/'){
                safecast_root = safecast_root.substr(0, safecast_root.length-1);
            }
            var root = "http://" + sfshost+ ":" + sfsport + safecast_root;
            var static_root = root + "/static";
            var mobile_root = root + "/mobile";
            console.log("Creating: " + static_root);
            console.log("Creating: " + mobile_root);
            for(i=0; i<feeds.length; i++){
                var device_path = root + "/static/" + feeds[i].Device_ID;
                console.log("Creating: " + device_path);
            }
        }
     );

}


//setupEnvironment();
var server= http.createServer(handle);
server.listen(1337, "192.168.1.101");
console.log('Server running at http://192.168.1.101:1337/');
