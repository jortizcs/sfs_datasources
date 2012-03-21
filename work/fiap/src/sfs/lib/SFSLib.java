package sfs.lib;

import java.io.*;
import java.net.*;
import java.util.Vector;
import org.json.*;
import java.util.StringTokenizer;

import org.json.*;

public class SFSLib{
	private String host = null;
	private int port = -1;
	private URL sfsurl = null;
	
	public SFSLib(String h, int p){
		try {
			host = h;
			port = p;
			sfsurl= new URL("http://"+h+":"+port);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public URL getSFSUrl(){
		return sfsurl;
	}

	public static void main(String[] args){
		SFSLib sfslib = new SFSLib("is4server.com", 8080);
		long now = -1;
		try {
			if(sfslib.exists("/is4/feeds/")){
				JSONObject nowObj = sfslib.getSFSTime();
				now = nowObj.getLong("Now");
				print("### Testing System time ###");
				print(new String("Now="+now));
				print("\n###Testing ts range query 1###");
				JSONObject qresults = sfslib.tsRangeQuery(
								"/is4/feeds/Dent_5PA_elt-E_Circuits_8_10-12_WP53/A_sensor_apparent_pf",
								now-60, false, 
								now, false);
				print(new String("qresults: " + qresults.toString()));
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public Vector<Object> mkrsrc(String path, String name, String type){

		JSONObject request = new JSONObject();
		try {
			if(type.equalsIgnoreCase("default")){
				request.put("operation", "create_resource");
				request.put("resourceName", name);
				request.put("resourceType", type);
			} else if(type.equalsIgnoreCase("devices")){
				request.put("operation", "create_resource");
				request.put("resourceName", name);
				request.put("resourceType", type);
			} else if(type.equalsIgnoreCase("device")){
				request.put("operation", "create_resource");
				request.put("resourceName", name);
				request.put("deviceName", name);
				request.put("resourceType", type);
			} else if(type.equalsIgnoreCase("genpub")){
				request.put("operation", "create_generic_publisher");
				request.put("resourceName", name);
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		
		try {
			URL u = new URL(sfsurl.toString() + path);
			return HttpOps.put(u, request.toString());
		} catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public  Vector<Object> mksmappub(String path, URL smapurl){
		JSONObject request = new JSONObject();
		try {
			request.put("operation", "create_smap_publisher");
			request.put("smap_urls", smapurl.toString());
		
			URL u = new URL(sfsurl.toString() + path);
			return HttpOps.put(u, request.toString());
		} catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

    public JSONObject getProps(String path){
        try {
            String url = new String(sfsurl.toString() + path);
            URL u = new URL(url);
            Vector<Object> resp = HttpOps.get(u);
            if(resp!=null && ((Integer)resp.get(0)).intValue()==200){
                JSONObject respObj = new JSONObject((String)resp.get(1));
                return respObj.getJSONObject("properties");
            } else {
                return null;
            }
        } catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
	
	public Vector<Object> overwriteProps(String path, String propsStr){
		JSONObject request = new JSONObject();
		JSONObject props = null;
		try {
			request.put("operation", "overwrite_properties");
			//props = (JSONObject)JSONSerializer.toJSON(propsStr);
			props = new JSONObject(propsStr);
			request.put("properties", props);
			URL u = new URL(sfsurl.toString() + path);
			return HttpOps.put(u, request.toString());
		} catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	public Vector<Object> updateProps(String path, String propsObj){
		JSONObject request = new JSONObject();
		JSONObject props = null;
		try {
			request.put("operation", "update_properties");
			//props = (JSONObject)JSONSerializer.toJSON(propsObj);
			props = new JSONObject(propsObj);

			request.put("properties", props);
			URL u = new URL(sfsurl.toString() + path);
			return HttpOps.put(u, request.toString());
		} catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	public boolean exists(String path){
		try {
			String url = new String(sfsurl.toString() + path);
			URL u = new URL(url);
			Vector<Object> v = HttpOps.get(u);
			if(v != null && v.size()>0)
				return true;
		} catch(Exception e){
			//e.printStackTrace();
		}
		return false;
	}
	
	public JSONObject tsQuery(String path, long timestamp){
		try {
			String url = new String(sfsurl.toString() + path + "?query=true&ts_timestamp=" + timestamp);
			URL u = new URL(url);
			Vector v = HttpOps.get(u);
			if(v!=null)
				//return (JSONObject)JSONSerializer.toJSON(v.get(1));
				return new JSONObject((String)v.get(1));
		} catch(Exception e){
			//e.printStackTrace();
		}
		return null;
	}
	
	public JSONObject tsRangeQuery(String path, long tslowerbound, boolean includelb, 
									long tsupperbound, boolean includeub){
		String queryParams = "?query=true&";
		if(includelb){
			queryParams = new String(queryParams+"ts_timestamp=gte:"+tslowerbound);
		} else {
			queryParams = new String(queryParams+"ts_timestamp=gt:"+tslowerbound);
		}
		
		if(includeub){
			queryParams =  new String(queryParams+",lte:"+tsupperbound);
		} else {
			queryParams = new String (queryParams+",lt:"+tsupperbound);
		}
		
		try {
			String url = new String(sfsurl.toString() + path + queryParams);
			URL u = new URL(url);
			Vector<Object> v = HttpOps.get(u);
			
			if(v!= null){
				//return (JSONObject)JSONSerializer.toJSON(v.get(1));
				return new JSONObject((String)v.get(1));
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public JSONObject tsNowRangeQuery(String path, long tslowerbound, boolean includelb, 
									long tsupperbound, boolean includeub){
		String queryParams = "?query=true&";
		if(includelb){
			queryParams = new String(queryParams+"ts_timestamp=gte:"+tslowerbound);
		} else {
			queryParams = new String(queryParams+"ts_timestamp=gt:"+tslowerbound);
		}
		
		if(includeub){
			queryParams =  new String(queryParams+",lte:"+tsupperbound);
		} else {
			queryParams = new String (queryParams+",lt:"+tsupperbound);
		}
		
		try {
			String url = new String(sfsurl.toString() + path + queryParams);
			URL u = new URL(url);
			Vector v= HttpOps.get(u);
			if(v!=null)
				//return (JSONObject)JSONSerializer.toJSON(v.get(1));
				return new JSONObject((String)v.get(1));
		} catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public JSONObject getSFSTime(){
		try {
			String url = new String(sfsurl.toString() + "/is4/time");
			URL u = new URL(url);
			Vector<Object> v = HttpOps.get(u);
			if(v!=null)
				//return (JSONObject)JSONSerializer.toJSON(v.get(1));
				return new JSONObject((String)v.get(1));
		} catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	
	public JSONArray getChildren(String path){
		try {
			String url = new String(sfsurl.toString() + path);
			URL u = new URL(url);
			Vector<Object> v = HttpOps.get(u);
			if(v!=null)
				//return ((JSONObject)JSONSerializer.toJSON(v.get(1))).getJSONArray("children");
				return (new JSONObject((String)v.get(1))).getJSONArray("children");
		} catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

    public void delete(String path){
        try{
            String url = new String(sfsurl.toString() + path);
            URL u = new URL(url);
            HttpOps.delete(u);
        } catch(Exception e){
        }
    }    
	
	public String createSymlink(String origin, String target) throws Exception, JSONException {
		StringTokenizer tokenizer = new StringTokenizer(origin, "/");
		Vector<String> tokens = new Vector<String>();
		while(tokenizer.hasMoreElements())
			tokens.add(tokenizer.nextToken());
        StringBuffer parentBuf = new StringBuffer();
        for(int i=0; i<tokens.size()-1; i++)
            parentBuf.append("/").append(tokens.elementAt(i));
        String url = new String(sfsurl.toString() + parentBuf.toString());
		URL u = new URL(url);

		JSONObject jsonObj = new JSONObject();
		jsonObj.put("operation", "create_symlink");
		jsonObj.put("name", tokens.lastElement());
		jsonObj.put("uri", target);
		return (String)HttpOps.put(u, jsonObj.toString()).elementAt(1);
	}

    public boolean putStreamData(String streamPath, String pubid, String dataObjStr) throws Exception, JSONException{
        try {
            if(streamPath==null || pubid==null || dataObjStr==null)
                return false;
            String url;
            URL u;
            JSONObject dataObj = new JSONObject(dataObjStr);
            if(dataObj.has("ts")){
                url = new String(sfsurl.toString() + streamPath + "?type=generic&addts=false&pubid=" + pubid);
                u = new URL(url);
            } else {
                url = new String(sfsurl.toString() + streamPath + "?type=generic&pubid=" + pubid);
                u = new URL(url);
            }

            //Put it
            Vector<Object> respVec = HttpOps.put(u, dataObjStr);
            if(respVec !=null && respVec.elementAt(0) == 200)
                return true;
        } catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }

    /*public String getType(String path){
        if(path==null)
            return null;
        String url = new String(sfsurl.toString() + path);
        URL u = new URL(url);
        Vector<Object> respVec = HttpOps.get(u);
        if(respVec !=null && ((Integer)respVec.get(0)).intValue()==200){
            JSONObject respObj = new JSONObject((String)respVec.get(1));
            if(respObj.has("pubid"))
                return "stream";
            return "default";
        }
        return null;
    }*/

    public static void print(String e){
        System.out.println(e);
    }
	
	
	
	
}
