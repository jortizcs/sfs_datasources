import java.lang.*;
import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.json.simple.*;
import org.json.simple.parser.*;

public class SmapToSfsDriver{
    private static JSONParser parser = new JSONParser();

    public static void main(String[] args){
        try {
            FileReader configFileReader = new FileReader(new File("config/smap_to_sfs_map.json"));
            BufferedReader bReader = new BufferedReader(configFileReader);
            StringBuffer fileData = new StringBuffer();
            String line =null;
            while((line=bReader.readLine())!=null)
                fileData.append(line);
            JSONArray configArray = (JSONArray)parser.parse(fileData.toString());

            for(int i=0; i<configArray.size(); i++){
                JSONObject thisEntry = (JSONObject)configArray.get(i);
                JSONObject smapInfoObj = (JSONObject)thisEntry.get("smap_info");
                JSONObject sfsInfoObj = (JSONObject)thisEntry.get("sfs_info");
                System.out.println(sfsInfoObj);
                ConnThread t = new ConnThread((String)smapInfoObj.get("repub_stream"),
                                                (String)smapInfoObj.get("uuid"),
                                                (String)sfsInfoObj.get("server"),
                                                (String)sfsInfoObj.get("path"),
                                                (String)sfsInfoObj.get("pubid"));
                t.start();
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public static class ConnThread extends Thread{
        public String smapServer = null;
        public String smapStreamUuid = null;
        public String sfsServer = null;
        public String sfsPathAndId =null;
        public static ConcurrentHashMap<String, ArrayList<JSONObject>> failBuffers = new ConcurrentHashMap<String, ArrayList<JSONObject>>();


        public ConnThread(String smapRepublishSvr, String streamUuid, String sfsSvr, 
                String sfsPath, String sfsStreamPubid){
            smapServer = smapRepublishSvr;
            smapStreamUuid = streamUuid;
            System.out.println(sfsSvr +"\n" + sfsPath + "\n" + sfsStreamPubid);
            sfsServer = sfsSvr;
            sfsPathAndId = sfsPath+"?type=generic&pubid="+ sfsStreamPubid;
        }

        public void run(){
            HttpURLConnection sfsConn = null;
            String smapQueryStr = "uuid=\"" + smapStreamUuid + "\"";
            HttpURLConnection smapConn = null;
            smapConn=setUpSmapConnection(smapConn, smapQueryStr);

            BufferedReader smapIn = null;

            while(true){
                try {
                    if(smapConn!=null){
                        if(smapIn==null)
                            smapIn = new BufferedReader(new InputStreamReader(smapConn.getInputStream()));

                        String line =null;
                        while((line=smapIn.readLine())!=null){
                            try {
                                JSONObject inputFromSmap = (JSONObject)parser.parse(line);
                                sendToStreamFS(inputFromSmap);
                            } catch(Exception e){
                                //e.printStackTrace();
                            }
                        }
                        
                    } else {
                        smapIn= null;
                        System.out.print("Could on connection to smap server: " + smapServer);
                        System.out.println(", trying again in 10 seconds...");
                        Thread.sleep(10000);
                        smapConn = setUpSmapConnection(smapConn, smapQueryStr);
                    }
                } catch(Exception e){
                    e.printStackTrace();
                    try {
                        smapIn.close();
                        smapIn= null;
                        smapConn.disconnect();
                        System.out.print("Could on connection to smap server: " + smapServer);
                        System.out.println(", trying again in 10 seconds...");
                        Thread.sleep(10000);
                        smapConn = setUpSmapConnection(smapConn, smapQueryStr);
                    } catch(Exception e2){
                        e2.printStackTrace();
                        System.out.println("Dying: " + smapStreamUuid + ", sfs_path=" + sfsPathAndId);
                        return;
                    }
                }
            }
        }

        public HttpURLConnection setUpSmapConnection (HttpURLConnection smapConn, String smapQueryStr){
            try {
                URL smapUrl = new URL("http://" + smapServer);
                smapConn = (HttpURLConnection)smapUrl.openConnection();
                smapConn.setRequestMethod("POST");
                smapConn.setRequestProperty("Content-Type", "text/plain");
                smapConn.setRequestProperty("Content-Length", Integer.toString(smapQueryStr.getBytes().length));
                smapConn.setUseCaches(false);
                smapConn.setDoInput(true);
                smapConn.setDoOutput(true);

                //System.out.println("POST_DATA=" + smapQueryStr);
                OutputStream out = smapConn.getOutputStream();
                out.write(smapQueryStr.getBytes(), 0, smapQueryStr.getBytes().length);
                out.flush();


                return smapConn;
            } catch(Exception e){
                e.printStackTrace();
                return null;
            }
        }

        public void sendToStreamFS(JSONObject smapDataObj){
            OutputStream out = null;
            HttpURLConnection sfsConn = null;
            JSONObject datapt=null;
            try {
                datapt = new JSONObject();
                Iterator keys = smapDataObj.keySet().iterator();
                String thisKey = (String)keys.next();
                JSONObject smapDp =(JSONObject) smapDataObj.get(thisKey);
                JSONArray smapReadings  = (JSONArray)smapDp.get("Readings");
                for(int i=0; i<smapReadings.size(); i++){
                    try {
                        URL sfsUrl = new URL("http://" + sfsServer + sfsPathAndId);
                        sfsConn = (HttpURLConnection)sfsUrl.openConnection();
                        sfsConn.setRequestMethod("POST");
                        sfsConn.setRequestProperty("Content-Type", "application/json");
                        //datapt.put("ts", ((JSONArray)smapReadings.get(i)).get(0));
                        datapt.put("value",((JSONArray)smapReadings.get(i)).get(1) );
                        sfsConn.setRequestProperty("Content-Length",Integer.toString(datapt.toString().getBytes().length));
                        sfsConn.setDoInput(true);
                        sfsConn.setDoOutput(true);
                        sfsConn.setConnectTimeout(1000);
                        sfsConn.setReadTimeout(1000);
                        out = sfsConn.getOutputStream();
                        System.out.println("Posting to: " + sfsUrl.toString() +"\n\tPOSTING:" + datapt);
                        out.write(datapt.toString().getBytes(), 0, datapt.toString().getBytes().length);
                        out.flush();
                        out.close();

                        InputStream is = sfsConn.getInputStream();
                        String line  = null;
                        BufferedReader r = new BufferedReader(new InputStreamReader(is));
                        while((line=r.readLine())!=null)
                            System.out.println("\t" + line);
                        is.close();
                        sfsConn.disconnect();
                    } catch(Exception e){
                        if(!(e instanceof SocketTimeoutException)){
                            e.printStackTrace();
                        } else{
                            System.out.println("\t::SocketTimeoutException::sfs_info=http://" + sfsServer + sfsPathAndId);
                            /*ArrayList<JSONObject> buffer = null;
                            if(!failBuffers.containsKey(sfsPathAndId)){
                                buffer = new ArrayList<JSONObject>();
                                buffer.add(datapt);
                            } else {
                                buffer = failBuffers.get(sfsPathAndId);
                                buffer.add(datapt);
                            }
                            failBuffers.put(sfsPathAndId, buffer);*/
                        }
                    } finally{
                        try {
                            out.close();
                            sfsConn.disconnect();
                        } catch(Exception e3){}
                    }
                }
            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}

