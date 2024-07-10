package tcss558.team05;

import java.io.Serializable;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This is the remote interface that is exposed to the client
 */
public interface RemoteHashtable extends Remote, Serializable {

    /**
     * gets the value stored at the specified key 
     * @param key The key for which the value is to be returned
     * @return The value at the specified key 
     * @throws RemoteException This is thrown when there is a communication error when the client tried to call this method remotely
     */
    public Serializable get(String key) throws RemoteException;

    /**
     * Store the specified value at the specified key
     * @param key The key at which the value is to be stored
     * @param value the value to be stored
     * @throws RemoteException This is thrown when there is a communication error when the client tried to call this method remotely
     */
    public void put(String key, Serializable value) throws RemoteException;

    /**
     * Removes the value at the specified key
     * @param key the key associated with the value to be removed
     * @return the removed values at the specified key
     * @throws RemoteException This is thrown when there is a communication error when the client tried to call this method remotely
     */
    public Serializable remove(String key) throws RemoteException;

}
