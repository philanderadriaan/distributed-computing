package tcss558.team05;

import java.io.Serializable;

import java.rmi.RemoteException;

import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is class represents a node virtual or actual. But usually used for virtual nodes
 */
public class VirtualNode implements Serializable{
    /**
     * This represents the data stored inside the node
     */
    Map<String, Serializable> storage;

    /**
     * The ids of the nodes that have backup of my storage
     */
    ArrayList<Integer> backingUpNodesIds;

    


    /**
     * The id of the node
     */
    int id;

    /**
     * Creates a node with an id
     * @param id id of the node
     */
    public VirtualNode(int id){
        this.id = id;
        storage = new HashMap<String, Serializable>();
    }

    /**
     * Creates a new VirtualNode instance with an ID and a predetermined storage. This is used when converting an ActualNode into a VirtualNode.
     * @param id
     * @param storage
     */
    public VirtualNode(int id, Map<String, Serializable> storage){
        this.id = id;
        if(storage!=null)
        this.storage = storage;
        else
        	this.storage = new HashMap<String, Serializable>();
        
    }

    /**
     * @inheritDoc
     */
    public synchronized void put(String key, Serializable value){
        storage.put(key, value);
        DhtLogger.log("Virtual Node"+id+": stored value at ["+ key+"]");
    }

    /**
     * @inheritDoc
     * */
    public Serializable get(String key) {
        DhtLogger.log("Virtual Node"+id+": getting value at ["+ key+"]");
        return storage.get(key);
    }

    /**
     * @inheritDoc
     */
    public synchronized Serializable remove(String key) {
        DhtLogger.log("Virtual Node"+id+": removing value at ["+ key+"]");
        return storage.remove(key);
    }

    /**
     * @inheritDoc
     */
    public int size() {
        // TODO Implement this method
        return 0;
    }

    /**
     * Get the node id
     * @return the node id
     */
    public int getId() {
        return id;
    }

    


    /**
     * get the data stored inside the node
     * @return data stored inside the node
     */
    public Map<String, Serializable> getStorage() {
        return storage;
    }

    /**
     * Adds one node id to the list of nodes that has back up to the storage in this node
     * @param nodeId
     */
    public synchronized void addBackingUpNodesIds(int nodeId) {
        this.backingUpNodesIds.add(nodeId);
    }


    /**
     * Removes a node id from the backing up nodes ids list
     * @param nodeId the id to be removed
     * @return the removed id
     */
    public synchronized int removeBackingUpNodesIds(int nodeId) {
        return this.backingUpNodesIds.remove(nodeId);
    }

    /**
     * Get all ids of nodes that has backup to the storage in this node
     * @return all ids of nodes that has backup to the storage in this node
     */
    public ArrayList<Integer> getBackingUpNodesIds() {
        return backingUpNodesIds;
    }


}
