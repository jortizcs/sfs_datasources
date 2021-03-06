var http = require('http');

var sfs_host="localhost";
var sfs_port=8080;

function sfsconnector(host, port){
    sfs_host = host;
    sfs_port = port;

    /**
     * Checks if the path exists on this streamfs server.
     * @param m_path the path to check
     * @param callback the function called after the GET request.  It is called with the status code
     *          of the get request.
     */
    this.exists = 
        function(m_path, callback){
            var options = {
                host: sfs_host,
                port: sfs_port,
                path: m_path,
                method: 'GET'
            };

            var req = http.get(options, 
                    function(res){
                        console.log("statusCode=" + res.statusCode);
                        f(res.statusCode);
                    }
                );
        };

    /**
     * Creates a streamfs default or stream file.
     * @param type default|stream
     * @param parentPath the path of the location to create the file.  It cannot be a path to a stream file.
     * @param nameOfFile the name of the file to create.
     * @param callback a callback function called with the status and an associated message or with the status code
     *          of the request.
     */
    this.createFile = 
        function(type, parentPath, nameOfFile, callback){
            var put_data = new Object();
            if(type == 'default'){
                put_data.operation = 'create_resource';
                put_data.resourceType = 'default';
            } else if(type=='stream'){
                put_data.operation = 'create_generic_publisher';
            } else {
                var msg = "createFile::unknown type error";
                callback('error', msg);
                return;
            }
            put_data.resourceName = nameOfFile;
            var put_dataStr = JSON.stringify(put_data);

            var options = {
                host: sfs_host,
                port: sfs_port,
                path: parentPath,
                method: 'PUT',
                headers: {
                    'Content-Type':'application/json',
                    'Content-Length':put_dataStr.length
                }
            };

            var req = http.request(options, 
                function(res){
                    callback(res.statusCode);        
                }
            );
            
            req.write(put_dataStr);
            req.end();
        };

    /**
     * Delete a file in streamfs.
     */
    this.deleteFile = 
        function(m_path, callback){
            var options = {
                host: sfs_host,
                port: sfs_port,
                path: m_path,
                method: 'DELETE',
            };

            var req = http.request(options, 
                function(res){
                    callback(res.statusCode);        
                }
            );

            req.end();
        };

    /**
     * Overwrite the properties of a file in streamfs.
     */
    this.overwriteProps = 
        function(m_path, props, callback){
            var owPropsRequest = new Object();
            owPropsRequest.operation = 'overwrite_properties';
            owPropsRequest.properties = props;
            var propsStr = JSON.stringify(owPropsRequest);
            var options = {
                host: sfs_host,
                port: sfs_port,
                path: m_path,
                method: 'POST',
                headers: {
                    'Content-Type':'application/json',
                    'Content-Length':propsStr.length
                }
            };

            var req = http.request(options, 
                function(res){
                    res.on('data', 
                        function(chunk){
                            callback(res.statusCode, chunk);
                    });
                }
            );

            req.write(propsStr);
            req.end();
        };

    /**
     * Change the location of an existing file in streamfs.
     */
    this.move = 
        function(fromPath, toPath, callback){
            var moveReq = new Object();
            moveReq.operation = 'move';
            moveReq.src = fromPath;
            moveReq.dst = toPath;
            var moveReqStr = JSON.stringify(moveReq);

            var options = {
                host: sfs_host,
                port: sfs_port,
                path: fromPath,
                method: 'PUT',
                headers: {
                    'Content-Type':'application/json',
                    'Content-Length':moveReqStr.length
                }
            };

            var req = http.request(options, 
                function(res){
                    res.on('data', 
                        function(chunk){
                            callback(res.statusCode, chunk);
                    });
                }
            );

            req.write(moveReqStr);
            req.end();
        };
}


/*var sfs = new sfsconnector("133.11.168.9", 8080);
var callback = function(statusCode){
    console.log("This is what I heard: " + statusCode);
    if(statusCode==201){

        var props = new Object();
        props.test='test properties set here';
        props.units='jUnits';
        sfs.overwriteProps('/temp', props, callback);
    }
};

//sfs.createFile('default', '/', 'temp', callback);

sfs.deleteFile('/temp', callback);*/
