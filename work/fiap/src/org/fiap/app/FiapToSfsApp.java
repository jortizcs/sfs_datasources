package org.fiap.app;

import org.fiap.soap.FIAPStorageStub;
import org.apache.axis2.databinding.types.URI;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;
import java.util.StringTokenizer;
import java.lang.NumberFormatException;
import java.net.URL;
import java.util.Timer;
import java.util.TimerTask;

import sfs.lib.HttpOps;
import sfs.lib.SFSLib;

public class FiapToSfsApp {
    private static int ptcnt=0;
    private static String host = "203.178.135.99";
    private static int port = 8080;
    private static SFSLib sfs_server=null;

    private static HashMap<String, String> streamToPubid=null;
    private static JSONObject mapping =null;
    private static JSONArray points=null;
    private static JSONObject valuemap = null;

    private static int periodSec = 60;
    private static HashMap<String, Boolean> valueMapSet = new HashMap<String, Boolean>();
    private static HashMap<String, Long> lastTsSeen = new HashMap<String, Long>();

    public static void deleteAll(){
        try {
            java.util.Iterator keysIt = mapping.keys();
            while(keysIt.hasNext()){
                String thisKey =  (String)keysIt.next();
                JSONObject o = mapping.getJSONObject(thisKey);
                String hardlink =o.getString("hardlink");
                JSONArray symlinks = o.optJSONArray("symlinks");
                if(symlinks!=null && symlinks.length()>0){
                    for(int i=0; i<symlinks.length(); i++){
                        sfs_server.delete((String)symlinks.get(i));
                        System.out.println("Delete:" + (String)symlinks.get(i));
                    }
                }
                sfs_server.delete(hardlink);
                System.out.println("Delete:"+ hardlink);
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public static JSONObject loadValueMap(){
        try {
            BufferedReader in= new BufferedReader(new FileReader("strvalToNum.json"));
            StringBuffer pointListContent = new StringBuffer();
            String thisline = null;
            while((thisline = in.readLine())!= null)
                pointListContent.append(thisline);
            return new JSONObject(pointListContent.toString());
        } catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static JSONObject loadPointList(){
        try {
            BufferedReader in= new BufferedReader(new FileReader("pointlist.json"));
            StringBuffer pointListContent = new StringBuffer();
            String thisline = null;
            while((thisline = in.readLine())!= null)
                pointListContent.append(thisline);
            return new JSONObject(pointListContent.toString());
        } catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static JSONObject loadMapping(){
        try {
            BufferedReader in= new BufferedReader(new FileReader("mapping.json"));
            StringBuffer pointListContent = new StringBuffer();
            String thisline = null;
            while((thisline = in.readLine())!= null)
                pointListContent.append(thisline);
            return new JSONObject(pointListContent.toString());
        } catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void fetch(FIAPStorageStub.Query query){
        try {
            /* Header*/ 
			FIAPStorageStub.Header header=new FIAPStorageStub.Header(); 
			header.setQuery(query); 
			
			/* Transport*/ 
			FIAPStorageStub.Transport transport=new FIAPStorageStub.Transport(); 
			transport.setHeader(header);
			
			/* QueryRQ */ 
			FIAPStorageStub.QueryRQ queryRQ=new FIAPStorageStub.QueryRQ(); 
			queryRQ.setTransport(transport);
			
			/* Stub*/
			FIAPStorageStub server=new FIAPStorageStub("http://fiap-storage.gutp.ic.i.u-tokyo.ac.jp/axis2/services/FIAPStorage"); 
			FIAPStorageStub.QueryRS queryRS=server.query(queryRQ);
			
			/* queryRS Transport */ 
			transport=queryRS.getTransport(); 
			
			/* Transport Header */ 
			header=transport.getHeader(); 
			if(header==null){ 
				System.err.println("Fatal Error: Header object was not found."); 
				System.exit(0); 
			} 
			
			/* Header OK, Error */ 
			FIAPStorageStub.OK ok=header.getOK(); 
			FIAPStorageStub.Error error=header.getError();
			
			if(ok==null){
				if(error!=null){ 
					System.err.println("Error: type=\'"+error.getType()+"\'; message=\'"+error.getString()+"\'"); 
					System.exit(0); 
				} else{ 
					System.err.println("Fatal Error: Neither OK nor Error object was not found."); 
					System.exit(0); 
				}
			}
			
			FIAPStorageStub.Body body=transport.getBody(); 
			if(body==null){ 
				System.err.println("Info: No data were returned."); 
				System.exit(0); 
			}
			FIAPStorageStub.Point[] point=body.getPoint(); 
            System.out.println("point[].length=" + point.length);
			for(int i=0;point!=null && i<point.length;i++){ 
				String id=point[i].getId().toString(); 
				System.out.println("# "+id); 
				FIAPStorageStub.Value[] value=point[i].getValue(); 
				for(int t=0;t<value.length;t++){ 
					String time=value[t].getTime().getTime().toString(); 
					String content=value[t].getString(); 
                    ptcnt +=1;
					//System.out.println(ptcnt + "\t\""+time+"\",\""+content+"\"");

                    JSONObject obj = mapping.getJSONObject(id);
                    String sfs_hardlink = obj.getString("hardlink");
                    if(sfs_hardlink != null ){
                        String pubid = streamToPubid.get(sfs_hardlink);
                        if(pubid!=null){
                            System.out.println(ptcnt + "\t\""+
                                (value[t].getTime().getTimeInMillis()/1000)+
                                "\",\""+content+"\"");
                            JSONObject datapt = new JSONObject();
                            Long ts = (value[t].getTime().getTimeInMillis()/1000);
                           
                            try {
                                Long lastSeenTs = lastTsSeen.get(sfs_hardlink);
                                if(ts.longValue() >0 && (lastSeenTs==null || lastSeenTs.longValue()!=ts.value())){
                                    if(!valuemap.has(content))
                                        Double.parseDouble(content);//if this errors out, it's not a number or it's null
                                    else{
                                        String strval = content;
                                        content = new Double(valuemap.getDouble(content)).toString();

                                        //record it in the properites so we know how to interpret the values
                                        if(!valueMapSet.containsKey(sfs_hardlink)){
                                            JSONObject props = sfs_server.getProps(sfs_hardlink);
                                            if(props!=null && !props.has(strval)){
                                                JSONObject newprops = new JSONObject();
                                                JSONObject map = new JSONObject();
                                                map.put(strval, new Double(content).doubleValue());
                                                newprops.put("value_map", map);
                                                sfs_server.updateProps(sfs_hardlink, newprops.toString());
                                                valueMapSet.put(sfs_hardlink, new Boolean(true));
                                                System.out.println("Set props with value-map:" + sfs_hardlink);
                                            }
                                        }
                                    }

                                    datapt.put("ts", ts);
                                    datapt.put("value", new Double(content).doubleValue());
                                    System.out.println("POST " + datapt.toString());
                                    sfs_server.putStreamData(sfs_hardlink, pubid, datapt.toString());
                                    lastTsSeen.put(sfs_hardlink, new Long(ts));
                                }
                            } catch(Exception e){
                                if(e instanceof NumberFormatException){
                                    System.err.println(content + " is not a number; no map available");
                                } else {
                                    e.printStackTrace();
                                }
                            }
                        }else {
                            System.err.println("error::pubid null for path:" + sfs_hardlink);
                        }

                    } else{
                        System.err.println("error::hardlink null for id:" + id);
                    }
				} 
			} 
        } catch(Exception e){
            e.printStackTrace();
        }
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
            sfs_server = new SFSLib(host, port);
            streamToPubid = new HashMap<String, String>();
            mapping = loadMapping();
            valuemap = loadValueMap();
            JSONObject pointlistObj = loadPointList();
            points = pointlistObj.getJSONArray("points");
            //deleteAll();
            ArrayList<FIAPStorageStub.Key> keyList = new ArrayList<FIAPStorageStub.Key>();
			/* Keys */

            HashMap<String, String> uniqueKeys = new HashMap<String, String>();
            for(int i=0; i<points.length(); i++){
                try {
                    if(mapping.has(points.getString(i)) && !uniqueKeys.containsKey(points.getString(i))){
                        FIAPStorageStub.Key key1=new FIAPStorageStub.Key();
                        key1.setId(new URI(points.getString(i)));
                        key1.setAttrName(FIAPStorageStub.AttrNameType.time); 
                        key1.setSelect(FIAPStorageStub.SelectType.maximum); 
                        keyList.add(key1);
                        uniqueKeys.put(points.getString(i), "");
                        makeSFSPath(mapping,points.getString(i));
                    }
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
            System.out.println("keyList.size()=" + keyList.size());

            //schedule a query task every minute
			Timer timer = new Timer();
            QueryTask queryTask = new QueryTask(keyList);
            timer.schedule(queryTask, 0L, periodSec*1000);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

    public static void makeSFSPath(JSONObject mapping, String path){
        try {
            //create the associated resource in sfs
            //fetch the associated mapping object (contains associated hardlink and symlink path)

            JSONObject sfsMapObj = mapping.getJSONObject(path);
            String hardlink = sfsMapObj.getString("hardlink");
            String type= sfsMapObj.getString("type");
            JSONArray symlinks=sfsMapObj.optJSONArray("symlinks");
           
            //check if the hardlink exists, if not, create it
            if(!sfs_server.exists(hardlink)){
                Vector<String> tokens = new Vector<String>();
                StringTokenizer tokenizer = new StringTokenizer(hardlink,"/");
                while(tokenizer.hasMoreTokens())
                    tokens.add(tokenizer.nextToken());
                StringBuffer buildPathBuf = new StringBuffer("/");
                for(int i=0; i <tokens.size()-1; i++){
                    String parent = buildPathBuf.toString();
                    if(i==0)
                        buildPathBuf.append(tokens.elementAt(i));
                    else 
                        buildPathBuf.append("/").append(tokens.elementAt(i));
                    String newPath = buildPathBuf.toString();
                    if(!sfs_server.exists(newPath)){
                        sfs_server.mkrsrc(parent, tokens.elementAt(i), "default");
                        System.out.println("mkrsrc:[parent=" + parent + ", name=" + tokens.elementAt(i) + ", type=default]");
                    }
                }
                if(type.equals("stream")){
                    sfs_server.mkrsrc(buildPathBuf.toString(), tokens.lastElement(),"genpub");
                    System.out.println("mkrsrc:[parent=" + buildPathBuf.toString() + ", name=" + tokens.lastElement() + ", type=genpub]");
                    
                    //call get on the genpub and record it's newly assigned id
                    Vector<Object> respVec = HttpOps.get(new URL("http://" + host + ":" + port + hardlink));
                    if(respVec!=null && ((Integer)respVec.get(0)).intValue()==200){
                        String respStr = (String)respVec.get(1);
                        JSONObject respObj = new JSONObject(respStr);
                        String pubid = respObj.getString("pubid");
                        streamToPubid.put(hardlink, pubid);
                    }
                } else {//default
                    sfs_server.mkrsrc(buildPathBuf.toString(), tokens.lastElement(), "default");
                    System.out.println("mkrsrc:[parent=" + buildPathBuf.toString() + ", name=" + tokens.lastElement() + ", type=default]");
                }
            } else if(type.equals("stream")){
                //it exists, record its pubid if it's a stream
                Vector<Object> respVec = HttpOps.get(new URL("http://" + host + ":" + port + hardlink));
                if(respVec!=null && ((Integer)respVec.get(0)).intValue()==200){
                    String respStr = (String)respVec.get(1);
                    JSONObject respObj = new JSONObject(respStr);
                    String pubid = respObj.getString("pubid");
                    streamToPubid.put(hardlink, pubid);
                }
            }

            //check the symlinks, create them if necessary
            if(symlinks !=null){
                for(int i=0; i<symlinks.length(); i++){
                    String symlink = (String)symlinks.get(i);
                    if(symlink !=null && !sfs_server.exists(symlink)){
                        Vector<String> tokens = new Vector<String>();
                        StringTokenizer tokenizer = new StringTokenizer(symlink,"/");
                        while(tokenizer.hasMoreTokens())
                            tokens.add(tokenizer.nextToken());
                        StringBuffer buildPathBuf = new StringBuffer("/");
                        for(int j=0; j <tokens.size()-1; j++){
                            String parent = buildPathBuf.toString();
                            if(j==0)
                                buildPathBuf.append(tokens.elementAt(j));
                            else 
                                buildPathBuf.append("/").append(tokens.elementAt(j));
                            String newPath = buildPathBuf.toString();
                            if(!sfs_server.exists(newPath)){
                                sfs_server.mkrsrc(parent, tokens.elementAt(j), "default");
                                System.out.println("mkrsrc:[parent=" + parent + ", name=" + tokens.elementAt(j) + ", type=default]");
                            }
                        }
                        
                        System.out.println("createSymlink:[source=" + symlink + ", target="+hardlink + "]");
                        sfs_server.createSymlink(symlink, hardlink);
                    }
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static class QueryTask extends TimerTask{
        private ArrayList<FIAPStorageStub.Key> keyList=null;

        public QueryTask(ArrayList<FIAPStorageStub.Key> k){
            super();
            keyList = k;
        }

        public void run(){
            /* Query*/ 
			FIAPStorageStub.Query query=new FIAPStorageStub.Query(); 
            FIAPStorageStub.Uuid uuid=new FIAPStorageStub.Uuid(); 
            uuid.setUuid(java.util.UUID.randomUUID().toString()); 
            query.setId(uuid); 
            query.setType(FIAPStorageStub.QueryType.storage); 
            
            int hidx = 0;
            int lidx = 0;
            int fetchCallCnt = 0;

            while(hidx<=keyList.size()){
                if(hidx<keyList.size() && hidx-lidx<50){
                    FIAPStorageStub.Key key = keyList.get(hidx);
                    query.addKey(key); 
                    hidx+=1;
                } else {
                    fetch(query);
                    lidx = hidx-1;
                    hidx +=1;
                    fetchCallCnt +=1;

                    //re-create the query.
                    query=new FIAPStorageStub.Query(); 
                    uuid=new FIAPStorageStub.Uuid(); 
                    uuid.setUuid(java.util.UUID.randomUUID().toString()); 
                    query.setId(uuid); 
                    query.setType(FIAPStorageStub.QueryType.storage); 
                }
            }
        }
    }
	
}
