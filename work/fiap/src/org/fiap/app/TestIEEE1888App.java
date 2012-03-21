package org.fiap.app;
import org.fiap.soap.FIAPStorageStub;
import org.apache.axis2.databinding.types.URI;
import org.json.JSONObject;
import org.json.JSONArray;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.util.ArrayList;

public class TestIEEE1888App {

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
			// FIAPStorageStub server=new FIAPStorageStub("http://fiap-dev.gutp.ic.i.u-tokyo.ac.jp/axis2/services/FIAPStorage");
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
					System.out.println("\""+time+"\",\""+content+"\""); 
				} 
				System.out.println(); 
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
            JSONObject pointlistObj = loadPointList();
            JSONArray points = pointlistObj.getJSONArray("points");
            ArrayList<FIAPStorageStub.Key> keyList = new ArrayList<FIAPStorageStub.Key>();
			/* Keys */
            for(int i=0; i<points.length(); i++){
                try {
                    if(points.getString(i).contains("102B1")){
                        FIAPStorageStub.Key key1=new FIAPStorageStub.Key();
                        key1.setId(new URI(points.getString(i)));
                        key1.setAttrName(FIAPStorageStub.AttrNameType.time); 
                        key1.setSelect(FIAPStorageStub.SelectType.maximum); 
                        keyList.add(key1);
                    }
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
            System.out.println("keyList.size()=" + keyList.size());

			/* Query*/ 
			FIAPStorageStub.Query query=new FIAPStorageStub.Query(); 
			FIAPStorageStub.Uuid uuid=new FIAPStorageStub.Uuid(); 
			uuid.setUuid(java.util.UUID.randomUUID().toString()); 
			query.setId(uuid); 
			query.setType(FIAPStorageStub.QueryType.storage); 
            for(int i=0; i<keyList.size(); i++){
                FIAPStorageStub.Key key1 = keyList.get(i);
			    query.addKey(key1); 
            }
            fetch(query);
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
