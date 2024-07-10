package tcss558.team05;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

import tcss558.team05.ActualNode.EventType;

/**
 * This interface defines the methods that needs to called internally between nodes. 
 */
public interface InternalMessenger extends RemoteHashtable {
    
    /**
     * Handles a request join received from a new node that wants to join the ring. 
     * The new node will receive an available id and a set of virtual nodes to manage. The new actual node has to confirm that it has 
     * completed the join process by calling the joinCompleted(int nodeId) method
     * @return a set of nodes that the new actual node is going to manage
     * @throws RemoteException
     */
    public VirtualNode[] handleJoinRequest() throws RemoteException;
    
    /**
     * This methods gets called to confirm that a new actual node whose id is nodeId has succesfully joined the ring. 
     * This has to be synchronized as it is updating a variable that might be used 
     * by multiple threads
     * @param nodeId the id of the new node that has joined the ring
     * @param joinHandlerNodes the node Ids to which the join task is delegated. Join delegation happens when a node doesn't have virtual nodes then it requests from its successor. This continues to get populated as the delegation continues
     * @return the final list of successors to which the join task was delegated. After the join process is done, this list is going to be used to notify all nodes involved that the rebalancing is done.
     * @throws RemoteException
     */
    public void joinCompleted(int nodeId) throws RemoteException;
    
    /**
     * Get the id of this node
    * @return the id of this node
    */
    public int getId() throws RemoteException;
    
    /**
     * Update the neighbours of certain actual node specified by targetNodeId knowning that a new actual node (updatedNodeId) has been created
     * @param targetNodeId The actual node that needs to update its neighbours list 
     * @param updateNodeId the actual node that has been created
     * @throws RemoteException thrown when remote communication error occurs
     */
    public void updateNeighbour(int targetNodeId, int updateNodeId, EventType eventType) throws RemoteException;
    /**
     * Get string showning virtual nodes and neighbours of this node
     * @return string showning virtual nodes and neighbours of this node
     * @throws RemoteException thrown when there is remote communication error
     */
    public String getDetails() throws RemoteException;

    /**
     * Get the virtual nodes that managed by this node
     * @return the virtual nodes that managed by this node
     * @throws RemoteException thrown when there is remote communication error
     */
    public Map<Integer,VirtualNode> getVirtualNodes() throws RemoteException;
    /**
     * Get successor of this node
     * @return successor of this node
     * @throws RemoteException thrown when there is remote communication error
     */
    public InternalMessenger getSuccessor() throws RemoteException;

    /**
     * This methods stores a backup of certain node
     * @param nodeId the id of the node to be backed up
     * @param storage the data to be backed up
     * @throws RemoteException thrown when there is some a remote communication error 
     */
    public void addBackup(int nodeId, Map<String,Serializable> storage) throws RemoteException;
    
    /**
     * This methods stores a backup of certain node
     * @param nodeId the id of the node to be backed up
     * @param storage the data to be backed up
     * @throws RemoteException thrown when there is some a remote communication error 
     */
    public void setBackup(Map<Integer, Map<String, Serializable>> backup) throws RemoteException;
    
    /**
     * Gets the number of all actual nodes in the network. 
     * @param requesterId the ID of the node that requests to know the size
     * @return the number of all actual nodes in the network
     * @throws RemoteException thrown when there is some a remote communication error 
     */
    public int getNetworkActualSize(int requesterId) throws RemoteException;
    
    /**
     * Gets the number of all actual nodes in the network. This method will call getNetworkActualSize(int requesterId) and will pass the node's own id as parameter
     * @return the number of all actual nodes in the network
     * @throws RemoteException thrown when there is some a remote communication error 
     */
    public int getNetworkActualSize() throws RemoteException;

    /**
     * This method handles the leave request from a node that is declaring to leave the network. It basically merge the virtual nodes of the leaving node to
     * its list of virtual nodes and then convert the leaving node into a virtual node and it adds it also to its list.
     * @param leavingNodeId the ID of the node that is leaving
     * @throws RemoteException thrown when there is some a remote communication error
     * @throws Exception thrown when a node other than the successor of this node calls this method. Only the successor of this node can call this method.
     */
    public void handleLeaveRequest(int leavingNodeId) throws RemoteException, Exception;

    /**
     * Get the storage of this actual node. This is used when the node declares to leave the network.
     * @throws RemoteException thrown when there is a remote communication error
     */
    public Map<String, Serializable> getStorage() throws RemoteException;
    
    /**
     * This method does nothing and is only used to ping certain actual node checking if it is dead or alive
     */
    public void ping() throws RemoteException;
    
    /**
     * backs up the value in the parameter on the predecessor of the node calling the method.
     * @param nodeId The ID of the node calling the method. This could be an ID of a virtual node inside an actual node
     * @param key the key associated with the value for which the backup is to be taken
     * @param value the value for which the backup is to be taken
     * @throws RemoteException thrown when there is a remote communication error 
     */
    public void addBackup(int nodeId, String key, Serializable value) throws RemoteException;
    
    /**
     * Remove a value from the backup. This is usually called when the remove method is called.
     * @param nodeId The ID of the node calling the method. This could be an ID of a virtual node inside an actual node
     * @param key the key associated with the value to be removed from the backup
     * @throws RemoteException RemoteException thrown when there is a remote communication error 
     */
    public void removeBackup(int nodeId, String key) throws RemoteException;
    
    /**
     * returns all values backed up in this node in the form of a map of maps
     * @return a map pf maps representing the values backed up associated with nodes' IDs (actual and virtual) for which the backup was taken
     * @throws RemoteException
     */
    public Map<Integer, Map<String, Serializable>> getBackup() throws RemoteException;

    
    
}
