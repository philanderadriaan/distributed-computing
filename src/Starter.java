   package tcss558.team05;

   import java.io.IOException;

   import java.net.Socket;

   import java.rmi.NotBoundException;
   import java.rmi.Remote;
   import java.rmi.RemoteException;
   import java.rmi.registry.LocateRegistry;
   import java.rmi.registry.Registry;
   import java.rmi.server.RMIClientSocketFactory;
   import java.rmi.server.RMISocketFactory;
   import java.rmi.server.UnicastRemoteObject;

   import java.util.Arrays;
   import java.util.Scanner;

/**
 * This class is used to start the RMI registry, start the first actual node in the ring network and start any other actual nodes that will join the network.
 *
 */
   public class Starter {
   
    /**
     * This is the port that is used by clients to call put, get and other hashtable service methods
     */
      static int rmiPort;
   
    /**
     * An RMI registry through which communication with the outside world occur.
     */
      static Registry rmiRegistry;
   
    /**
     * Starts an RMI registry and keep printing the list of nodes it contains to the screen.
     * @throws RemoteException thrown when there is an error in the remote communication
     */
      private Starter() throws RemoteException {
         rmiRegistry = LocateRegistry.createRegistry(rmiPort);
         while (true) {
            //System.out.println(Arrays.toString(rmiRegistry.list()));
            String[] nodes = rmiRegistry.list();
            String output = "";
            for (String node : nodes) {
               output += node + ": ";
               try {
                  InternalMessenger m = (InternalMessenger) rmiRegistry.lookup(node);
                  output += m.getDetails() + ",\n";
               } 
                  catch (Exception e) {
                     output+= "(dead),\n";
                  }
            }
            System.out.println(output);
         
            try {
               Thread.sleep(1000);
            } 
               catch (InterruptedException e) {
               }
         }
      }
   
    /**
     * In this main method, it is possible start the RMI registry, start the first actual node in the ring network and start any other actual nodes that will join the network.
     * @param args arguments can be:
     *  (rmi <port>) to start the rmi registry at certain port
     *  (actualnode <rmi address> <rmi port number> <node id> <m>) to start an actual node
     * @throws RemoteException
     */
      public static void main(String[] args) throws RemoteException {
        //args = new String[]{"rmi", "8005"};
         //args = new String[] { "actualnode", "localhost", "8005", "0", "1" };
      
         ActualNode current = null;
         if (args.length > 0) {
            if ("rmi".equalsIgnoreCase(args[0])) {
               if (args.length != 2) {
                  System.out.println("Argument is <port number>");
               }
               try {
                  rmiPort = Integer.valueOf(args[1]);
               } 
                  catch (NumberFormatException e) {
                     System.out.println("Port number must be numerical");
                  }
               new Starter();
            } 
            else if ("actualnode".equalsIgnoreCase(args[0])) {
               if (args.length != 5) {
                  System.out.println("Arguments must be <rmi address> <rmi port number> <node id> <m>");
                  System.exit(0);
               }
               try {
                  rmiRegistry = LocateRegistry.getRegistry(args[1], Integer.valueOf(args[2]));
                  if (rmiRegistry.list().length == 0) {
                     current = new ActualNode(args[1], Integer.valueOf(args[2]), Integer.valueOf(args[3]),
                                       Integer.valueOf(args[4]));
                  } 
                  else {
                	  current = new ActualNode(Integer.valueOf(args[2]), args[1], Integer.valueOf(args[3]),
                                       Integer.valueOf(args[4]));
                  }
               } 
                  catch (NumberFormatException e) {
                     System.out.println("Port, ID and m must be numerical.");
                     System.exit(0);
                  }
            } 
            else {
               System.out.println("First argument must be RMI or ACTUALNODE");
               System.exit(0);
            }
         } 
         else {
            System.out.println("Arguments must be RMI <port number> or ACTUALNODE <rmi address> <rmi port number> <node id> <m>");
            System.exit(0);
         }
         waitForLeaveCommand(current);
      }
   
      public static void startFirstNode(String rmiHostname, int port, int nodeId, int m) throws RemoteException {
         new ActualNode(rmiHostname, port, nodeId, m);
      
      
      }
   
      public static void waitForLeaveCommand(ActualNode node) {
         Scanner sc = new Scanner(System.in);
         System.out.println("Please press 'y' to leave the ring: ");
         String str = sc.nextLine();
         while (true) {
            if (str.toLowerCase().equals("y")) {
               node.leave();
               System.exit(0);
            }
            sc.nextLine();
         }
      }
   
   }
