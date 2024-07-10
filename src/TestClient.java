package tcss558.team05;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This class is used to test the RemoteHashtable
 */
public class TestClient {


    public static void main(String[] args) {

        try 
        {
            /*
             * The below code tests the put method asynchronously*/
            Registry reg = LocateRegistry.getRegistry("localhost", 8005);
            RemoteHashtable map = (RemoteHashtable) reg.lookup("dht-node0");

            //AsyncExecutor.execute(map, "put", "asf", "1");
            for(int i=0;i<30; i++)
            {
            	//AsyncExecutor.execute(map, "remove", i +"");
            	System.out.println(map.get(""+i));
            	//AsyncExecutor.execute(map, "put", i +"", i);
            }
            /*AsyncExecutor.execute(map, "put", "asf", "hasan");
            AsyncExecutor.execute(map, "put", "asf2", "111");*/
            


        } catch (Exception e) 
        {
            System.out.println(e.getMessage());
        }
        //System.exit(0);

    }
    
    public TestClient() {
        
    }
    
    public String getAsyncString(String name) {
        return name + " Hello async";
    }
    
    public void callback(Serializable result) {
        System.out.println(result);
    }

}
