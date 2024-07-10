package tcss558.team05;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;

/**
 * This class represent an actual node in the Ring of Chords network. It acts as
 * a RemoteHashtable as well.
 */
public class ActualNode extends UnicastRemoteObject implements InternalMessenger {

    /**
     * @author Hasan Asfoor This is an enumeration that represents the type of
     *         event that can happen on an actual node. It can be node joining,
     *         leaving or failure.
     */
    public enum EventType {
	JOIN, LEAVE, FAIL
    }

    /**
     * The ID of the successor
     */
    int successorId = -1;

    /**
     * The ID of the predecessor
     */
    int predecessorId = -1;
    /**
     * This variable is set when this node declares to leave
     */
    boolean isLeaving = false;

    /**
     * This variable is set when the node is processing the leave request after
     * a node declares that it is leaving the network. It is set to false after
     * the leave request is completed handled.
     */
    boolean isHandlingLeave = false;

    /**
     * This variable is set when has been detected as dead. After handling the
     * failure, the variable is set to false
     */
    boolean isHandlingCrash = false;

    /**
     * This is a lock to be used for synchronization when a request method
     * (get/put/remove) waits for the join operation to complete
     */
    final Object joinLock = new Object();

    /**
     * This is a lock to be used for synchronization when a request method
     * (get/put/remove) waits for the join operation to complete
     */
    final Object requestLock = new Object();

    /**
     * This is a lock to be used for synchronization when a request method
     * (get/put/remove) waits for leave handling operation to complete
     */
    final Object leaveHandlerLock = new Object();

    /**
     * This is a lock is used for synchronization when a request method
     * (get/put/remove) waits for the node to rebalance as a result of
     * discovering a dead node
     */
    final Object crashHandlerLock = new Object();

    /**
     * This is used to indicate whether the system is stable or not. if it is 0
     * then this node is stable. Otherwise, it is unstable because this node is
     * handling a join request
     */
    Integer currentJoinRequests = 0;

    /**
     * This is usd to indicate whether this node is responding to a client
     * request or not. 0 means no client requests are being processed. This is
     * mainly used in the leave method so that it can wait until the node
     * finishes serving all requests.
     */
    Integer currentClientRequests = 0;

    /**
     * The id of this actual node
     */
    int id;

    /**
     * The virtual nodes
     */
    Map<Integer, VirtualNode> virtualNodes;

    /**
     * This contains all the keys/values that map to the id of the actual node
     */
    Map<String, Serializable> storage;

    /**
     * This represents all th neighbour nodes to this node. In a ring with m=2,
     * each node has two neighbours. When m=3, then each node has three
     * neighbours and so on. Each neighbour object represents an actual node
     * that is pointed to by this node. It also acts as a proxy object to pass
     * messages to the actual node it is representing. A neighbour can be looked
     * up by its nodeId.
     */
    Map<Integer, InternalMessenger> neighbours;

    /**
     * The actual node next to me in the ring.
     */
    InternalMessenger successor;

    /**
     * The actual node before me in the ring.
     */
    InternalMessenger predecessor;

    /**
     * This is the size of the network. the number of nodes in the network are
     * 2^m.
     */
    int m;

    /**
     * This is the port that is used by clients to call put, get and other
     * hashtable service methods
     */
    int rmiPort;

    /**
     * This is the host of the registry.
     */
    String rmiHostname;

    /**
     * An RMI registry through which communication with the outside world occur.
     * It also has reference for other nodes to enable this node to figure out
     * its neighbours, predecessors and successors
     */
    Registry rmiRegistry;

    /**
     * This is a table used for backing up the successor. It basically stores
     * all values in the successor and associate it with its id. The values
     * stored in the virtual nodes of the successor are also stored here but
     * associated with the virtual nodes' ids and not the successors' ids.
     */
    Map<Integer, Map<String, Serializable>> backups;

    /**
     * This constructor is used to start the first node in the ring.
     * 
     * @param rmiHostname
     *            the hostname of the machine on which the RMI registry is
     *            running
     * @param port
     *            the port on which the RMI registry is running
     * @param nodeId
     *            the ID that is going to be assigned to this node when it
     *            starts
     * @param m
     *            The size of the network is 2^m
     * @throws RemoteException
     *             thrown when there is a remote communication error
     */
    public ActualNode(String rmiHostname, int port, int nodeId, int m) throws RemoteException {
	super(nodeId);
	if (nodeId < 0 || nodeId >= Math.pow(2, m)) {
	    DhtLogger.log("Node ID must be between 0 and " + (((int) Math.pow(2, m)) - 1));
	    System.exit(0);
	}
	this.rmiHostname = this.rmiHostname;
	this.m = m;
	rmiPort = port;
	initRmiRegistry();
	try {
	    rmiRegistry.bind("dht-node" + this.getId(), this);
	} catch (AlreadyBoundException e) {
	    DhtLogger.log("Node " + this.getId() + " already used.");
	    System.exit(0);
	}
	initVitualNodes();
	storage = new HashMap<String, Serializable>();
	backups = new HashMap<Integer, Map<String, Serializable>>();
	DhtLogger.log("Node" + id + ": has been started");
	DhtLogger.log("Node" + id + ": " + this.getDetails());
    }

    /**
     * @inheritDoc
     */
    @Override
    public int getId() {
	return id;
    }

    /**
     * This method is only called by the first node in the ring network. It
     * basically creates all the virtual nodes that can exist in the network
     */
    private void initVitualNodes() {
	virtualNodes = new HashMap<Integer, VirtualNode>();
	int ringSize = (int) Math.pow(2, m);
	for (int start = (id + 1) % ringSize; start != id; start = (start + 1) % ringSize) {
	    virtualNodes.put(start, new VirtualNode(start));
	}

    }

    /**
     * Get an virtual node managed by this actual node
     * 
     * @param nodeId
     *            the id of the virtual node requested
     * @return an object representing the virtual node that is requested
     */
    public VirtualNode getVirtualNode(int nodeId) {
	return virtualNodes.get(nodeId);
    }

    /**
     * Adds a virtual node to be managed by this actual node. This usually
     * happens when a node failure is detected which causes the dead node to
     * become a virtual node managed by one of its backup nodes. This has to be
     * synchronized as it is updating a variable that might be used by multiple
     * threads
     * 
     * @param vNode
     */
    public synchronized void addVirtualNode(VirtualNode vNode) {
	virtualNodes.put(vNode.getId(), vNode);
    }

    /**
     * removes a virtual node from the set of virtual nodes managed by this
     * actual node. This is done when a new actual node requests to join the
     * network.
     * 
     * @param nodeId
     *            the id of the virtual node to be removed
     * @return An object representing the virtual node that has been removed.
     */
    public VirtualNode removeVirtualNode(int nodeId) {
	return virtualNodes.remove(nodeId);
    }

    /**
     * This method blocks any request in case the node has declared to leave the
     * network. As a result, any request calling this method is never going to
     * be satisfied as it will fall into a never ending wait and the process
     * will eventually terminate.
     */
    public void blockRequestIfLeaving() {
	if (isLeaving) {
	    synchronized (this) {
		DhtLogger.log("Node" + id + ": process requests anymore because the node is leaving!");
		try {
		    wait();
		} catch (InterruptedException e) {
		}
	    }
	}
    }

    /**
     * @inheritDoc
     */
    @Override
    public void put(String key, Serializable value) throws RemoteException {
	blockRequestIfLeaving();
	currentClientRequests++;
	DhtLogger.log("Node" + id + ": Starting storing value at [" + key + "]");

	try {

	    while (currentJoinRequests > 0) {
		synchronized (joinLock) {
		    DhtLogger.log("Node" + id + ": can't put at [" + key + "] now... waiting for join process");
		    joinLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for joining process. Continue putting at [" + key + "]");
		}
	    }

	    while (isHandlingLeave) {
		synchronized (leaveHandlerLock) {
		    DhtLogger.log("Node" + id + ": can't put at [" + key + "] now... waiting for leave handling process to complete");
		    leaveHandlerLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for leave handling process. Continue putting at [" + key + "]");
		}
	    }

	    while (isHandlingCrash) {
		synchronized (crashHandlerLock) {
		    DhtLogger.log("Node" + id + ": can't put at [" + key + "] now... waiting for rebalancing to complete after discovering dead node");
		    leaveHandlerLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for rebalancing because of dead node. Continue putting at [" + key + "]");
		}
	    }

	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	}
	int nodeId = resolveHashcode(key);
	DhtLogger.log("Node" + id + ": The ID of the node to store the value is " + nodeId);
	if (nodeId == id) {
	    storage.put(key, value);
	    storeBackup(id, key, value);
	    DhtLogger.log("Node" + id + ": stored value at [" + key + "]");
	} else if (containsVirtualNode(nodeId)) {
	    getVirtualNode(nodeId).put(key, value);
	    storeBackup(nodeId, key, value);
	} else {

	    do {
		InternalMessenger nearest = findNearestNeighbour(nodeId);
		try {
		    nearest.put(key, value);
		    break;
		    // AsyncExecutor.execute(nearest, "put", key, value);
		} catch (Exception e) {
		    // DhtLogger.log(e.getMessage());

		    if (e instanceof java.rmi.ConnectException) {
			try {
			    // reactToNodeFailure(getNeighbourId(nearest));
			    reactToNodeFailure();
			} catch (Exception e1) {
			    // TODO Auto-generated catch block
			    e1.printStackTrace();
			}
		    }

		}
	    } while (true);

	}
	DhtLogger.log("Node" + id + ": Completed storing value at [" + key + "]");
	synchronized (requestLock) {
	    currentClientRequests--;
	    requestLock.notifyAll();
	}

    }

    /**
     * store a backup of a value that is located in certain node
     * 
     * @param nodeId
     *            the ID of the actual node that has the value to be backed up
     * @param key
     *            the key at which the value is stored
     * @param value
     *            the value to be backed up
     */
    public void storeBackup(int nodeId, String key, Serializable value) {
	DhtLogger.log("Node" + id + ": Storing a backup for node"+nodeId+" ["+key+":"+value+"]");
	if (predecessor != null) {
	    try {
		predecessor.addBackup(nodeId, key, value);
	    } catch (RemoteException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

    /**
     * Removes a values from the backup
     * 
     * @param nodeId
     *            the ID of the node for which the backed up value is to be
     *            removed
     * @param key
     *            the key associated with the value to be removed
     */
    public void clearBackup(int nodeId, String key) {
	DhtLogger.log("Node" + id + ": Removing backup value for node"+nodeId+" at ["+key+"]");
	if (predecessor != null) {
	    try {
		predecessor.removeBackup(nodeId, key);
	    } catch (RemoteException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

    /**
     * Gets the ID of the neighbour given an InternalMessenger pointing to the
     * actual node. This method doesn't use RMI
     * 
     * @param neighbour
     *            the InternalMessenger instance representing a neighbour
     * @return the node ID of the neighbour
     */
    private int getNeighbourId(InternalMessenger neighbour) {
	Iterator<Entry<Integer, InternalMessenger>> iter = neighbours.entrySet().iterator();
	while (iter.hasNext()) {
	    Entry<Integer, InternalMessenger> entry = iter.next();
	    if (entry.getValue().equals(neighbour)) {
		return entry.getKey();
	    }
	}
	return -1;
    }

    /**
     * get the node id that should handle the key
     * 
     * @param hashcode
     *            the key string
     * @return the id of the node that should manage the value at the given key
     */
    private int resolveHashcode(String key) {
	int ringSize = (int) Math.pow(2, m);
	int nodeId = key.hashCode() % ringSize;
	if (nodeId < 0)
	    nodeId = ringSize + nodeId;
	return nodeId;
    }

    /**
     * @inheritDoc
     * */
    @Override
    public Serializable get(String key) throws RemoteException {
	blockRequestIfLeaving();
	currentClientRequests++;
	Serializable result = null;
	DhtLogger.log("Node" + id + ": Starting getting value at [" + key + "]");
	try {

	    while (currentJoinRequests > 0) {
		synchronized (joinLock) {
		    DhtLogger.log("Node" + id + ": can't get at [" + key + "] now... waiting for join process");
		    joinLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for joining process. Continue getting at [" + key + "]");
		}
	    }

	    while (isHandlingLeave) {
		synchronized (leaveHandlerLock) {
		    DhtLogger.log("Node" + id + ": can't get at [" + key + "] now... waiting for leave handling process to complete");
		    leaveHandlerLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for leave handling process. Continue getting at [" + key + "]");
		}
	    }

	    while (isHandlingCrash) {
		synchronized (crashHandlerLock) {
		    DhtLogger.log("Node" + id + ": can't get at [" + key + "] now... waiting for rebalancing to complete after discovering dead node");
		    leaveHandlerLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for rebalancing because of dead node. Continue getting at [" + key + "]");
		}
	    }

	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	}

	int nodeId = resolveHashcode(key);

	if (nodeId == id) {
	    result = storage.get(key);

	} else if (containsVirtualNode(nodeId)) {
	    result = getVirtualNode(nodeId).get(key);
	} else {
	    DhtLogger.log("Node" + id + ": go to nearest neighbour and return value at [" + key + "]");
	    do {
		InternalMessenger nearest = findNearestNeighbour(nodeId);
		try {
		    result = nearest.get(key);
		    break;
		} catch (Exception e) {
		    // DhtLogger.log(e.getMessage());

		    if (e instanceof java.rmi.ConnectException) {
			try {
			    reactToNodeFailure();
			} catch (Exception e1) {
			    // TODO Auto-generated catch block
			    e1.printStackTrace();
			}
		    }

		}
	    } while (true);

	}

	DhtLogger.log("Node" + id + ": returnning value at [" + key + "]");
	synchronized (requestLock) {
	    currentClientRequests--;
	    requestLock.notifyAll();
	}
	return result;
    }

    /**
     * @inheritDoc
     */
    public Serializable remove(String key) {
	blockRequestIfLeaving();
	currentClientRequests++;
	DhtLogger.log("Node" + id + ": Starting removing value at [" + key + "]");
	Serializable result = null;
	try {

	    while (currentJoinRequests > 0) {
		synchronized (joinLock) {
		    DhtLogger.log("Node" + id + ": can't remove at [" + key + "] now... waiting for join process");
		    joinLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for joining process. Continue removing at [" + key + "]");
		}
	    }

	    while (isHandlingLeave) {
		synchronized (leaveHandlerLock) {
		    DhtLogger.log("Node" + id + ": can't get at [" + key + "] now... waiting for leave handling process to complete");
		    leaveHandlerLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for leave handling process. Continue getting at [" + key + "]");
		}
	    }

	    while (isHandlingCrash) {
		synchronized (crashHandlerLock) {
		    DhtLogger.log("Node" + id + ": can't remove at [" + key + "] now... waiting for rebalancing to complete after discovering dead node");
		    leaveHandlerLock.wait();
		    DhtLogger.log("Node" + id + ": done waiting for rebalancing because of dead node. Continue removing at [" + key + "]");
		}
	    }

	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	}

	int nodeId = resolveHashcode(key);

	if (nodeId == id) {

	    result = storage.remove(key);
	    clearBackup(nodeId, key);

	} else if (containsVirtualNode(nodeId)) {
	    result = getVirtualNode(nodeId).remove(key);
	    clearBackup(nodeId, key);
	} else {
	    do {
		InternalMessenger nearest = findNearestNeighbour(nodeId);
		try {

		    DhtLogger.log("Node" + id + ": go to neighbour and remove value at [" + key + "]");
		    result = nearest.remove(key);
		    break;
		} catch (Exception e) {
		    // DhtLogger.log(e.getMessage());

		    if (e instanceof java.rmi.ConnectException) {
			try {
			    reactToNodeFailure();
			} catch (Exception e1) {
			    // TODO Auto-generated catch block
			    e1.printStackTrace();
			}
		    }
		}
	    } while (true);
	}
	DhtLogger.log("Node" + id + ": removed value at [" + key + "]");
	synchronized (requestLock) {
	    currentClientRequests--;
	    requestLock.notifyAll();
	}
	return result;
    }

    /**
     * Initialize the RMI registry so that this node can register itself in the
     * registry upon its start
     */
    private void initRmiRegistry() {
	try {
	    RMIClientSocketFactory socket = new RMIClientSocketFactory() {

		@Override
		public Socket createSocket(String host, int port) throws IOException {
		    Socket socket = RMISocketFactory.getDefaultSocketFactory().createSocket(host, port);
		    socket.setSoTimeout(10000);
		    return socket;
		}
	    };
	    rmiRegistry = LocateRegistry.getRegistry(rmiHostname, rmiPort, socket);
	    System.out.println("Started Node " + this.getId());
	} catch (RemoteException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Create an actual node and let it join another actual node
     * 
     * @param port
     *            the port on which the network is running
     * @param hostname
     *            the hostname of the RMI registry
     * @param targetNodeId
     *            the id of a node in the ring network
     * @param m
     *            a number that defines the size of the network which is 2^m
     * @throws RemoteException
     *             thrown when there is a problem accessing the remote node
     */
    public ActualNode(int port, String hostname, int targetNodeId, int m) throws RemoteException {
	super(0);
	this.m = m;
	rmiPort = port;
	rmiHostname = hostname;
	DhtLogger.log("Initializing the node...");
	initRmiRegistry();
	virtualNodes = new HashMap<Integer, VirtualNode>();
	storage = new HashMap<String, Serializable>();
	backups = new HashMap<Integer, Map<String, Serializable>>();
	try {
	    InternalMessenger channel = join(targetNodeId);
	    DhtLogger.log("Node" + id + ": start rebalancing nodes...");

	    DhtLogger.log("Node" + id + ": " + this.getDetails());
	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	}
    }

    /**
     * This method notifies nodes that are pointing to this node that some event
     * happened on this node and that they need to update their list of
     * neighbours, successors and predecessors accordingly
     * 
     * @param eventType
     *            the type of the event which can be join, leave of node failure
     */
    public void notifyDependantNodes(EventType eventType) {
	notifyDependantNodes(id, EventType.JOIN);

    }

    /**
     * Get IDs of all actual nodes nodes whose neighbour is this actual node
     * 
     * @param nodeId
     *            the ID of the node for which the pointing nodes to be
     *            discovered
     * @return IDs of all actual nodes nodes whose neighbour is this actual node
     */
    public List<Integer> getNodesPointingToNode(int nodeId) {
	int id = nodeId;
	List<Integer> result = new ArrayList<Integer>();
	int ringSize = (int) Math.pow(2, m);
	for (int i = 0; i < m; i++) {
	    int num = (id - (int) Math.pow(2, i));
	    if (num < 0)
		num = ((num % ringSize) + ringSize) % ringSize;
	    else
		num = (id - (int) Math.pow(2, i)) % ringSize;
	    try {
		int actId = discoverActualNodeId(num);
		if (nodeId != actId && !result.contains(actId))
		    result.add(actId);
	    } catch (RemoteException e) {
		// DhtLogger.log(e.getMessage());
	    }
	}
	return result;
    }

    /**
     * notify a set of nodes that are pointing to the affected node (joining or
     * leaving node) that the affected node has joined or left the network and
     * their neighbours need to be updated accordingly.
     * 
     * @param affectedNodeId
     *            the ID of the node joining or leaving the network
     * @param eventType
     *            the type of the event which can be join, leave of node failure
     */
    public void notifyDependantNodes(int affectedNodeId, EventType eventType) {

	Map<Integer, Integer> actualNodesToNotify = new HashMap<Integer, Integer>();
	try {
	    List<Integer> vNodes = getVirtualNodesOfNode(affectedNodeId);
	    // add the nodes pointing to my virtual nodes
	    for (int i : vNodes) {
		List<Integer> deptNodes = getNodesPointingToNode(i);
		for (int n : deptNodes)
		    actualNodesToNotify.put(n, n);
	    }
	    // add the nodes pointing directly to me
	    List<Integer> deptNodes = getNodesPointingToNode(affectedNodeId);
	    for (int n : deptNodes)
		actualNodesToNotify.put(n, n);
	} catch (Exception e2) {
	    // TODO Auto-generated catch block
	    e2.printStackTrace();
	}

	// Adding the successor of the updated node (joining or leaving node) to
	// the list of nodes to notify.
	String[] allActualNodes;
	try {
	    allActualNodes = rmiRegistry.list();
	    int predecessoraffectedNodeId = -1;

	    int[] actualNodesList = new int[allActualNodes.length];
	    int i = 0;
	    for (String nodeaffectedNodeIdStr : allActualNodes) {
		int nodeaffectedNodeId = Integer.parseInt(nodeaffectedNodeIdStr.replace("dht-node", ""));
		actualNodesList[i] = nodeaffectedNodeId;
		i++;
	    }
	    Arrays.sort(actualNodesList);
	    int successoraffectedNodeId = -1;
	    for (i = 0; i < actualNodesList.length; i++) {
		if (affectedNodeId == actualNodesList[i]) {
		    successoraffectedNodeId = actualNodesList[(i + 1) % actualNodesList.length];
		    break;
		} else if (affectedNodeId < actualNodesList[i]) {
		    successoraffectedNodeId = actualNodesList[i];
		    break;
		} else if (i == actualNodesList.length - 1 && affectedNodeId > actualNodesList[i])
		    successoraffectedNodeId = actualNodesList[0];

	    }
	    if (successoraffectedNodeId != id) // add the affectedNodeId to be
					       // notified as long as
					       // it is not my affectedNodeId.
					       // This node is
					       // expected to have updated its
					       // neighbours and no need to
					       // update them
					       // again
		actualNodesToNotify.put(successoraffectedNodeId, successoraffectedNodeId);
	} catch (RemoteException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	}
	actualNodesToNotify.remove(id);// The notifier must exclude itself from
				       // the notification!
	for (Map.Entry<Integer, Integer> entry : actualNodesToNotify.entrySet()) {
	    int deptaffectedNodeId = entry.getKey();
	    InternalMessenger nearest = findNearestNeighbour(deptaffectedNodeId);
	    try {
		nearest.updateNeighbour(deptaffectedNodeId, affectedNodeId, eventType);
	    } catch (Exception e) {

	    }
	}

    }

    /**
     * @inheritDoc
     */
    @Override
    public void updateNeighbour(int targetNodeId, int updateNodeId, EventType eventType) throws RemoteException {
	DhtLogger.log("Node" + id + ": updating the neighbours of node" + targetNodeId + " as a result of node" + updateNodeId + " joining/leaving the ring network.");

	int previousSuccessor = successorId;

	int ringSize = (int) Math.pow(2, m);
	if (updateNodeId == id)
	    return;
	if (targetNodeId == id || containsVirtualNode(targetNodeId)) {
	    createNeighbours();
	    // Creating empty virtual nodes as a replacement to the lost actual
	    // node
	    if (eventType == EventType.FAIL) {
		int i = (id + 1) % ringSize;
		for (; i != successorId; i = (i + 1) % ringSize) {
		    if (!virtualNodes.containsKey(i)) {
			// virtualNodes.put(i, new VirtualNode(i));
			Map b = backups.get(i);
			VirtualNode vNode = new VirtualNode(i, b);
			virtualNodes.put(i, vNode);
			predecessor.addBackup(i, b);
		    }
		}

		Map<Integer, VirtualNode> vNodesOfSuccessor = successor.getVirtualNodes();
		backups.clear();
		backups.put(successorId, successor.getStorage());
		for (Map.Entry<Integer, VirtualNode> vNodeEntry : vNodesOfSuccessor.entrySet()) {
		    backups.put(vNodeEntry.getKey(), vNodeEntry.getValue().getStorage());
		}
		/*
		 * try { reactToNodeFailure(); } catch (Exception e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */
	    }

	    DhtLogger.log(this.getDetails());
	} else {
	    InternalMessenger neighbour = findNearestNeighbour(targetNodeId);
	    neighbour.updateNeighbour(targetNodeId, updateNodeId, eventType);
	}
    }

    /**
     * This method creates neighbours (of type InternalMessenger) for this
     * actual node. This includes successor and predecessor. This has to be
     * synchronized because if two threads try to create neighbours at the same
     * time then it might result in creating wrong neighbours!
     */
    public synchronized void createNeighbours() {
	neighbours = new TreeMap<Integer, InternalMessenger>();

	try {
	    int successorId = -1;
	    int predecessorId = -1;

	    int[] actualNodesList = getActualNodesIDs();
	    int myIndex = Arrays.binarySearch(actualNodesList, id);
	    successorId = actualNodesList[(myIndex + 1) % actualNodesList.length];
	    if (myIndex == 0)
		predecessorId = actualNodesList[actualNodesList.length - 1];
	    else
		predecessorId = actualNodesList[myIndex - 1];

	    this.successorId = successorId;
	    this.predecessorId = predecessorId;
	    successor = (InternalMessenger) rmiRegistry.lookup("dht-node" + successorId);
	    predecessor = (InternalMessenger) rmiRegistry.lookup("dht-node" + predecessorId);
	    int ringSize = (int) Math.pow(2, m);
	    for (int k = 0; k < m; k++) {
		int neighbourNodeId = (int) (id + Math.pow(2, k)) % ringSize;
		if (!containsVirtualNode(neighbourNodeId)) {
		    InternalMessenger neighbour = discoverActualNode(neighbourNodeId);
		    int nId = neighbour.getId();
		    if (nId != id) // I can't be myself's neighbour
			neighbours.put(nId, neighbour);
		}
	    }

	} catch (AccessException e) {
	    DhtLogger.log(e.getMessage());
	} catch (RemoteException e) {
	    DhtLogger.log(e.getMessage());
	} catch (NotBoundException e) {
	    DhtLogger.log(e.getMessage());
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	String out = "Node" + id + ": My neighbours are {";
	for (Map.Entry<Integer, InternalMessenger> entry : neighbours.entrySet()) {
	    out += entry.getKey() + ",";
	}
	out = out.substring(0, out.length() - 1) + "}";
	DhtLogger.log(out);

    }

    /**
     * Determine the nearest neighbour node to a node whose id is passed in the
     * parameters
     * 
     * @param nodeId
     *            the id of the node in question
     * @return the neighbour node closest to nodeId
     */
    public InternalMessenger findNearestNeighbour(int nodeId) {

	InternalMessenger nearest = neighbours.get(nodeId);
	if (nearest != null)
	    return nearest;
	int[] dependantsIds = new int[m];
	int ringSize = (int) Math.pow(2, m);
	for (int i = 0; i < m; i++) {
	    int num = (nodeId - (int) Math.pow(2, i));
	    if (num < 0)
		num = ((num % ringSize) + ringSize) % ringSize;
	    else
		num = (nodeId - (int) Math.pow(2, i)) % ringSize;
	    dependantsIds[i] = num;
	}

	for (Map.Entry<Integer, InternalMessenger> entry : neighbours.entrySet()) {
	    InternalMessenger node = entry.getValue();
	    Integer nId = entry.getKey();
	    try {

		Map<Integer, VirtualNode> vNodes = node.getVirtualNodes();
		for (int i = 0; i < dependantsIds.length; i++) {

		    if (nId == dependantsIds[i] || vNodes.containsKey(nId))
			return node;
		}
	    } catch (RemoteException e) {
	    }
	}
	return successor;
    }

    /**
     * Get the data stored in this actual node
     * 
     * @return the data stored in this actual node
     */
    @Override
    public Map<String, Serializable> getStorage() {
	return storage;
    }

    /**
     * Get the nodes pointed to by this actual node
     * 
     * @return the nodes pointed to by this actual node
     */
    public Map<Integer, InternalMessenger> getNeighbours() {
	return neighbours;
    }

    /**
     * Get the value of m
     * 
     * @return the value of m
     */
    public int getM() {
	return m;
    }

    /**
     * Get the port on which RMI is running
     * 
     * @return the port on which RMI is running
     */
    public int getRmiPort() {
	return rmiPort;
    }

    /**
     * @inheritDoc
     */
    @Override
    public synchronized void addBackup(int nodeId, String key, Serializable value) {
	Map<String, Serializable> nodeStorage = backups.get(nodeId);
	if (nodeStorage == null) {
	    nodeStorage = new TreeMap<String, Serializable>();
	    backups.put(nodeId, nodeStorage);
	}
	nodeStorage.put(key, value);
	String msg = "adding backup value of";// "Node"+id+": backing up a value in node"+
					      // successorId;
	if (nodeId != successorId)
	    msg += " virtual node" + nodeId + " inside ";
	DhtLogger.log("Node" + id + ": " + msg + " node" + successorId + " at [" + key + "].");
    }

    /**
     * @inheritDoc
     */
    @Override
    public synchronized void removeBackup(int nodeId, String key) throws RemoteException {
	Map<String, Serializable> nodeStorage = backups.get(nodeId);
	if (nodeStorage != null) {
	    nodeStorage.remove(key);
	    String msg = "removing backup value of";// "Node"+id+": backing up a value in node"+
						    // successorId;
	    if (nodeId != successorId)
		msg += " virtual node" + nodeId + " inside ";
	    DhtLogger.log("Node" + id + ": " + msg + " node" + successorId + " at [" + key + "].");
	}
    }

    /**
     * @inheritDoc
     */
    @Override
    public void addBackup(int nodeId, Map<String, Serializable> storage) {
	if (storage != null) {
	    Map<String, Serializable> nodeStorage = backups.get(nodeId);
	    if (nodeStorage == null) {
		nodeStorage = new TreeMap<String, Serializable>();
		backups.put(nodeId, nodeStorage);
	    }

	    for (Entry<String, Serializable> entry : storage.entrySet()) {
		nodeStorage.put(entry.getKey(), entry.getValue());
	    }

	    DhtLogger.log("Node" + id + ": virtual node" + nodeId + " has been backed up.");
	}
    }

    /**
     * get a backup of nodeId node
     * 
     * @param nodeId
     *            the id of the backed up node
     * @return the storage of nodeId
     */
    public Map<String, Serializable> getBackup(int nodeId) {
	return backups.get(nodeId);
    }

    /**
     * Checks if the virtual node contains a key.
     * 
     * @param nodeId
     *            ID of virtual node.
     * @return Whether the virtual node exists.
     */
    public boolean containsVirtualNode(int nodeId) {
	return virtualNodes.containsKey(nodeId);
    }

    /**
     * This method requests to leave the network. It then becomes a virtual node
     * in its predecessor and all of its virtual nodes go to its predecessor as
     * well. This method has to be synchronized to guarantee that rebalancing
     * doesn't fail
     * */
    public synchronized void leave() {
	DhtLogger.log("Node" + id + ": Starts leaving the network...");
	isLeaving = true;
	// Waiting for requests to be processed first
	while (currentClientRequests > 0) {
	    synchronized (requestLock) {
		try {
		    DhtLogger.log("Node" + id + ": Can't leave the network right now... waiting for requests to be processed!");
		    requestLock.wait();
		    DhtLogger.log("Node" + id + ": Done waiting for requests. Continue leaving the network..");
		} catch (InterruptedException e) {
		    // DhtLogger.log(e.getMessage());
		}
	    }
	}

	try {

	    DhtLogger.log("Node" + id + ": Removing from the RMI registry..");
	    rmiRegistry.unbind("dht-node" + id);
	    DhtLogger.log("Node" + id + ": Notifying the predecessor to handle the leave request..");
	    predecessor.handleLeaveRequest(id);

	    DhtLogger.log("Node" + id + ": The node is exitting the network and the process is terminating.");
	    System.exit(0);
	} catch (RemoteException e) {
	    // DhtLogger.log(e.getMessage());
	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	}

    }

    /**
     * request from an actual node to join its' ring network. This has to be
     * synchronized otherwise the rebalancing could fail to give correct results
     * 
     * @param nodeId
     *            the ID of the node to be joined. Any node can join the network
     *            from any other node.
     * @return A remote reference to the node that has been contacted for the
     *         join operation
     * @throws Exception
     *             This is thrown when the network is full or when there is a
     *             problem contacting the remote node to be joined.
     */
    public synchronized InternalMessenger join(int nodeId) throws Exception {
	DhtLogger.log("Joing the ring network at node" + nodeId);
	InternalMessenger channel = null;
	channel = (InternalMessenger) rmiRegistry.lookup("dht-node" + nodeId);
	int size = channel.getNetworkActualSize();
	int ringSize = (int) Math.pow(2, m);
	if (size >= ringSize) {
	    DhtLogger.log("Network is full!");
	    System.exit(1);
	}
	VirtualNode[] vNodes = channel.handleJoinRequest();

	if (vNodes != null && vNodes.length > 0) {
	    this.id = vNodes[0].getId();
	    this.storage = vNodes[0].getStorage();

	    for (int i = 1; i < vNodes.length; i++)
		virtualNodes.put(vNodes[i].getId(), vNodes[i]);
	    rmiRegistry.bind("dht-node" + this.id, this);
	    DhtLogger.log("Node" + id + ": acquired id " + id);

	    DhtLogger.log("Node" + id + ": identifing and creating my neighbours..");
	    createNeighbours();
	    DhtLogger.log("Node" + id + ": Notifying nodes that should be pointing at me..");
	    notifyDependantNodes(EventType.JOIN);

	    DhtLogger.log("Node" + id + ": finalizing the join process");
	    channel.joinCompleted(id);
	    DhtLogger.log("Node" + id + ": Successfully joined the network.");

	} else {
	    throw new Exception("Network is full!");

	}
	return channel;

    }

    /**
     * @inheritDoc
     */
    @Override
    public synchronized VirtualNode[] handleJoinRequest() throws RemoteException {
	currentJoinRequests++;
	DhtLogger.log("Node" + id + ": Received a join request...");
	DhtLogger.log("Node" + id + ": Starts handling join request.....");
	VirtualNode[] vNodes = null;
	if (virtualNodes.size() > 0) {

	    int vNodesSize = virtualNodes.size();
	    int numOfNodesToGive = vNodesSize - vNodesSize / 2;
	    vNodes = new VirtualNode[numOfNodesToGive];
	    int ringSize = (int) Math.pow(2, m);
	    int index = -1;
	    if (vNodesSize % 2 == 0)
		index = (id + numOfNodesToGive + 1) % ringSize;
	    else
		index = (id + numOfNodesToGive) % ringSize;
	    for (int i = 0; i < numOfNodesToGive; i++, index = (index + 1) % ringSize) {
		VirtualNode vNode = virtualNodes.get(index);
		vNodes[i] = vNode;
	    }
	} else {
	    do {
		try {
		    vNodes = this.getSuccessor().handleJoinRequest();
		    break;
		} catch (Exception e) {

		    // DhtLogger.log(e.getMessage());

		    if (e instanceof java.rmi.ConnectException) {
			try {
			    reactToNodeFailure();
			} catch (Exception e1) {
			    // TODO Auto-generated catch block
			    e1.printStackTrace();
			}
		    }
		}
	    } while (true);
	}

	return vNodes;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void joinCompleted(int nodeId) throws RemoteException {

	if (virtualNodes.size() > 0) {

	    int vNodesSize = virtualNodes.size();
	    int numOfNodesToGive = vNodesSize - vNodesSize / 2;
	    int ringSize = (int) Math.pow(2, m);

	    int index = -1;
	    if (vNodesSize % 2 == 0)
		index = (id + numOfNodesToGive + 1) % ringSize;
	    else
		index = (id + numOfNodesToGive) % ringSize;
	    DhtLogger.log("Node" + id + ": Taking backup of node"+nodeId);
	    backups = new HashMap<Integer, Map<String, Serializable>>();
	    for (int i = 0; i < numOfNodesToGive; i++, index = (index + 1) % ringSize) {
		VirtualNode vNode = virtualNodes.remove(index);
		addBackup(index, vNode.getStorage());
	    }
	    createNeighbours();

	    // Adding everything I have to become a backup in my new successor
	    Map<Integer, Map<String, Serializable>> backupOfSuccessor = new HashMap<Integer, Map<String, Serializable>>();
	    for (Map.Entry<String, Serializable> entry : storage.entrySet()) {
		backupOfSuccessor.put(id, storage);
	    }

	    for (Map.Entry<Integer, VirtualNode> entry : virtualNodes.entrySet()) {
		backupOfSuccessor.put(entry.getKey(), entry.getValue().getStorage());
	    }

	    successor.setBackup(backupOfSuccessor);

	    DhtLogger.log("Node" + id + ": completed rebalancing nodes.");
	} else {
	    this.getSuccessor().joinCompleted(nodeId);
	}

	DhtLogger.log("Node" + id + ": Node" + nodeId + " has successfully joined the ring network.");

	synchronized (joinLock) {
	    currentJoinRequests--;
	    joinLock.notifyAll();
	}
    }

    /**
     * @inheritDoc
     */
    public Map<Integer, VirtualNode> getVirtualNodes() {
	return virtualNodes;
    }

    /**
     * This method discovers the actual node associated with the node id that is
     * passed. the node id could be of an actual node or virtual node
     * 
     * @param nodeId
     * @return
     * @throws RemoteException
     * @throws NotBoundException
     */
    public InternalMessenger discoverActualNode(int nodeId) throws NotBoundException, RemoteException {

	int actualNodeId = discoverActualNodeId(nodeId);
	return (InternalMessenger) rmiRegistry.lookup("dht-node" + actualNodeId);

    }

    /**
     * Get string showning virtual nodes and neighbours of this node
     * 
     * @return string showning virtual nodes and neighbours of this node
     */
    public String toString() {
	String vNodeStr = "";
	String nbrStr = "";

	if (virtualNodes != null && virtualNodes.size() > 0) {
	    for (Map.Entry<Integer, VirtualNode> entry : virtualNodes.entrySet()) {
		VirtualNode n = entry.getValue();
		vNodeStr += n.getId();// + ",";
		// the loop is to be removed
		for (Entry<String, Serializable> vNodeVal : n.getStorage().entrySet()) {
		    vNodeStr += "[" + vNodeVal.getKey() + ":" + vNodeVal.getValue() + "] ";
		}
		vNodeStr += ",";
	    }
	    vNodeStr = vNodeStr.substring(0, vNodeStr.length() - 1);
	}

	if (neighbours != null && neighbours.size() > 0) {
	    for (Map.Entry<Integer, InternalMessenger> entry : neighbours.entrySet()) {
		InternalMessenger nbr = entry.getValue();
		try {
		    nbrStr += nbr.getId() + ",";
		} catch (Exception e) {
		    nbrStr += entry.getKey() + "(dead),";
		}
	    }
	    nbrStr = nbrStr.substring(0, nbrStr.length() - 1);
	}
	String successorId = "";
	String predecessorId = "";

	try {
	    successor.ping();
	    successorId = this.successorId + "";
	} catch (Exception e) {
	    successorId = this.successorId + "(dead)";
	}

	try {
	    predecessor.ping();
	    predecessorId = this.predecessorId + "";
	} catch (Exception e) {
	    predecessorId = this.predecessorId + "(dead)";
	}

	String backupStr = " backups{";
	for (Entry<Integer, Map<String, Serializable>> nodeBackupEntry : this.backups.entrySet()) {
	    Map<String, Serializable> nodeBackup = nodeBackupEntry.getValue();
	    backupStr += " node" + nodeBackupEntry.getKey() + "{";
	    for (Entry<String, Serializable> entry : nodeBackup.entrySet()) {
		backupStr += "[" + entry.getKey() + ":" + entry.getValue() + "] ";
	    }
	    backupStr += "} ";
	}
	backupStr += "} ";

	String storageStr = " storage{";
	for (Entry<String, Serializable> entry : storage.entrySet()) {
	    storageStr += "[" + entry.getKey() + ":" + entry.getValue() + "] ";
	}
	storageStr += "} ";
	return "ID {" + id + "} Virtual Nodes{" + vNodeStr + "} Neighbours{" + nbrStr + "} Successor{" + successorId + "} Predecessor{" + predecessorId + "} " + storageStr + backupStr;
    }

    /**
     * @inheritDoc
     */
    @Override
    public String getDetails() throws RemoteException {
	return toString();
    }

    /**
     * Get the id of the actual node that is managing the virtual node specified
     * in the parameter
     * 
     * @param nodeId
     *            the id of the virtual node in question
     * @return the id of the actual node managing the virtual in question
     * @throws RemoteException
     *             thrown when there is remote communication error
     */
    public int discoverActualNodeId(int nodeId) throws RemoteException {
	// DhtLogger.log("Node" + id + ": looking for node" + nodeId);

	int[] actualNodesIDs = null;
	try {
	    actualNodesIDs = getActualNodesIDs();
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	int ringSize = (int) Math.pow(2, m);
	int actualNodeId = -1;
	for (int i = 0; i < actualNodesIDs.length; i++) {
	    if (i == 0 && actualNodesIDs[i] > nodeId)
		return actualNodesIDs[actualNodesIDs.length - 1];
	    if (actualNodesIDs[i] == nodeId)
		return actualNodesIDs[i];
	    else if (actualNodesIDs[i] < nodeId)
		actualNodeId = actualNodesIDs[i];
	    else
		break;
	}
	return actualNodeId;
    }

    /**
     * This method returns a list of IDs of all actual nodes in the network
     * 
     * @return a list of IDs of all actual nodes in the network
     * @throws Exception
     *             thrown when there is a remote communication error
     */
    private int[] getActualNodesIDs() throws Exception {
	String[] allActualNodes = rmiRegistry.list();

	int successorId = -1;
	int predecessorId = -1;

	int[] actualNodesList = new int[allActualNodes.length];
	int i = 0;
	for (String nodeIdStr : allActualNodes) {
	    int nodeId = Integer.parseInt(nodeIdStr.replace("dht-node", ""));
	    actualNodesList[i] = nodeId;
	    i++;
	}
	Arrays.sort(actualNodesList);
	return actualNodesList;
    }

    /**
     * @inheritDoc
     */
    @Override
    public InternalMessenger getSuccessor() throws RemoteException {
	return successor;
    }

    /**
     * Remove id of a backup of a neighbour
     * 
     * @param neighbourId
     *            the id of the neighbour
     * @param idOfBackup
     *            the id to be removed.
     * @return
     */
    public int removeNeighbourBackupId(int neighbourId, int idOfBackup) {
	return -1;
    }

    /**
     * Return a list of IDs of nodes that has a backup of the given neighbour
     * 
     * @param neighbourId
     *            the ID of the neighbour whose backups are to be found
     * @return a list of IDs of nodes that has a backup of the given neighbour
     */
    public List<Integer> getBackupsOfNeighbour(int neighbourId) {
	return null;
    }


    /**
     * Get the target node and requests from it to store a backup of my data
     * (including virtual nodes data
     * 
     * @param targetNodeId
     *            the node on which the backup is to be stored
     */
    private void backupOfMyStorage(int targetNodeId) {

    }

    /**
     * @inheritDoc
     */
    @Override
    public int getNetworkActualSize(int requesterId) throws RemoteException {

	if (id == requesterId)
	    return 0;
	else
	    return 1 + successor.getNetworkActualSize(requesterId);
    }

    /**
     * @inheritDoc
     */
    @Override
    public int getNetworkActualSize() {
	if (successor == null)
	    return 1;
	else {
	    try {
		return 1 + successor.getNetworkActualSize(id);
	    } catch (RemoteException e) {
		// DhtLogger.log(e.getMessage());
		return 1;
	    }
	}
    }

    /**
     * @inheritDoc
     */
    @Override
    public synchronized void handleLeaveRequest(int leavingNodeId) throws RemoteException, Exception {
	DhtLogger.log("Node" + id + ": handling leave request from node" + leavingNodeId);
	isHandlingLeave = true;

	if (leavingNodeId != successor.getId())
	    throw new Exception("Illegal call to this method. Only successor can call handleLeaveRequest(int) of its predecessor");
	InternalMessenger leavingNode = successor;
	Map<Integer, VirtualNode> vNodes = leavingNode.getVirtualNodes();
	DhtLogger.log("Node" + id + ": updating the backup values of my predecessors");
	for (Map.Entry<Integer, VirtualNode> entry : vNodes.entrySet()) {
	    virtualNodes.put(entry.getKey(), entry.getValue());

	    Map b = backups.remove(entry.getKey());
	    if (predecessorId != -1 && predecessorId != id && b != null)
		predecessor.addBackup(entry.getKey(), b);

	}
	DhtLogger.log("Node" + id + ": Grabbing the backup values of the leaving node..");
	backups = leavingNode.getBackup();

	VirtualNode vNode = new VirtualNode(leavingNodeId, leavingNode.getStorage());
	virtualNodes.put(leavingNodeId, vNode);
	if (predecessorId != -1 && predecessorId != id)
	    predecessor.addBackup(leavingNodeId, vNode.getStorage());

	DhtLogger.log("Node" + id + ": Updating my neighbours.");
	createNeighbours();
	DhtLogger.log("Node" + id + ": Notifying nodes pointing to node" + leavingNodeId + " that it is leaving the network so that they update their list of neighbours.");
	if (successorId == id)
	    backups.clear();
	notifyDependantNodes(leavingNodeId, EventType.LEAVE);
	synchronized (leaveHandlerLock) {
	    isHandlingLeave = false;
	    leaveHandlerLock.notifyAll();
	}
    }

    /**
     * This method detect all dead nodes it is pointing to, removes the dead
     * nodes from the RMI registry and then rebalances (updates neighbours list,
     * successor and predecessor). After that notified all other nodes that are
     * pointing to the dead nodes.
     * 
     * @throws Exception
     *             thrown when an error occur when communicating with the RMI
     *             registry
     */
    private void reactToNodeFailure() throws Exception {
	isHandlingCrash = true;
	DhtLogger.log("Node" + id + ": dead node has been discovered!!");
	DhtLogger.log("Node" + id + ": reacting to node failure...");
	List<Integer> deadNodes = getAllDeadNeighbours();

	for (int deadNodeId : deadNodes) {
	    InternalMessenger node = (InternalMessenger) rmiRegistry.lookup("dht-node" + deadNodeId);
	    if (node != null) {
		rmiRegistry.unbind("dht-node" + deadNodeId);
		int previousSuccessor = successorId;
		int ringSize = (int) Math.pow(2, m);
		createNeighbours();
		if (previousSuccessor == deadNodeId) {
		    int i = (id + 1) % ringSize;
		    for (; i != successorId; i = (i + 1) % ringSize) {
			if (!virtualNodes.containsKey(i)) {
			    // virtualNodes.put(i, new VirtualNode(i));
			    Map b = backups.get(i);
			    virtualNodes.put(i, new VirtualNode(i, b));
			    predecessor.addBackup(i, b);
			}
		    }
		}

		Map<Integer, VirtualNode> vNodesOfSuccessor = successor.getVirtualNodes();
		backups.clear();
		backups.put(successorId, successor.getStorage());
		for (Map.Entry<Integer, VirtualNode> vNodeEntry : vNodesOfSuccessor.entrySet()) {
		    backups.put(vNodeEntry.getKey(), vNodeEntry.getValue().getStorage());
		}

	    }
	}

	DhtLogger.log("Node" + id + ": Notifying other nodes of the failure...");

	for (int deadNodeId : deadNodes) {
	    notifyDependantNodes(deadNodeId, EventType.FAIL);
	}

	DhtLogger.log("Node" + id + ": failure has been completely fixed and network has been rebalanced.");
	synchronized (crashHandlerLock) {
	    isHandlingCrash = false;
	    crashHandlerLock.notifyAll();
	}
    }

    /**
     * Returns IDs of all neighbour nodes that are dead. When a node detects a
     * dead node, it then calls this method to detect if there are also other
     * dead nodes and fix itself
     * 
     * @return IDs of all neighbour nodes that are dead
     */
    private List<Integer> getAllDeadNeighbours() {
	List<Integer> deadNodes = new ArrayList<Integer>();
	String output = "";
	for (Map.Entry<Integer, InternalMessenger> entry : neighbours.entrySet()) {
	    int nodeId = entry.getKey();
	    InternalMessenger neighbour = entry.getValue();
	    try {
		neighbour.ping();
	    } catch (Exception e) {
		// //DhtLogger.log(e.getMessage());
		if (e instanceof java.rmi.ConnectException) {
		    if (!deadNodes.contains(nodeId)) {
			deadNodes.add(nodeId);
			output += "" + nodeId + ",";
		    }
		}
	    }
	}

	try {
	    successor.ping();
	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	    if (e instanceof java.rmi.ConnectException) {
		if (!deadNodes.contains(successorId)) {
		    deadNodes.add(successorId);
		    output += "" + successorId + ",";
		}
	    }
	}

	try {
	    predecessor.ping();
	} catch (Exception e) {
	    // DhtLogger.log(e.getMessage());
	    if (e instanceof java.rmi.ConnectException) {
		if (!deadNodes.contains(predecessorId)) {
		    deadNodes.add(predecessorId);
		    output += "" + predecessorId + ",";
		}

	    }
	}
	if (output.contains(","))
	    output = output.substring(0, output.length() - 2);
	if (deadNodes.size() > 0) {
	    DhtLogger.log("Node" + id + ": nodes {" + output + "} has been detected as dead nodes!");
	}
	return deadNodes;

    }

    /**
     * @inheritDoc
     */
    @Override
    public void ping() throws RemoteException {
	// this method does nothing and only used to check if the node is dead
	// or alive

    }

    /**
     * @inheritDoc
     */
    @Override
    public void setBackup(Map<Integer, Map<String, Serializable>> backup) throws RemoteException {
	backups = backup;

    }

    /**
     * @inheritDoc
     */
    @Override
    public Map<Integer, Map<String, Serializable>> getBackup() throws RemoteException {
	// TODO Auto-generated method stub
	return backups;
    }

    /**
     * Returns all IDs of all virtual nodes associated with certain actual node
     * 
     * @param nodeId
     *            the ID of the actual node
     * @return a list of all IDs of all virtual nodes associated with certain
     *         actual node
     * @throws Exception
     *             thrown when there is a remote communication error
     */
    private List<Integer> getVirtualNodesOfNode(int nodeId) throws Exception {
	// get the next node after node 4! and then return the nodes between 4
	// and 0. then iterate through them and find all dependent nodes on that
	// list and notify them all
	int[] nodes = getActualNodesIDs();
	int ringSize = (int) Math.pow(2, m);
	List<Integer> result = new ArrayList<Integer>();
	int i = (nodeId + 1) % ringSize;
	for (; Arrays.binarySearch(nodes, i) < 0; i = (i + 1) % ringSize) {
	    result.add(i);
	}
	return result;
    }

}
