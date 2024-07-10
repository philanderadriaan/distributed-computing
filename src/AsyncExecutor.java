package tcss558.team05;

import java.io.Serializable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This class is used to asyncronously execute any method in any object. 
 * Using this class will prevent writing threading code over and over and make asynchronous calls much easier
 */
public class AsyncExecutor implements Callable<Serializable>, Runnable{
    /**
     * An instance of any object
     */
    Object instance;

    /**
     * The method name to be called
     */
    String methodName;

    /**
     * The name of the method to execute after the asynchronous call is completed
     */
    String callbackMethodName;
    
    Future asyncResult;

    /**
     * The arguments to be passed to the method to be called
     */
    Object[] args;

    /**
     * create AsyncExecutor instance
     * @param obj the object whose method is to be called
     * @param methodName the method name to be called
     * @param arguments the arguments to be passed to the method
     * @throws Exception thrown when one of the methods don't exist in the object
     */
    public AsyncExecutor(Object obj, String methodName, Object... arguments) throws Exception {
        super();
        instance = obj;
        this.methodName = methodName;
        args = arguments;
        if(!isMethodExists(methodName))
        throw new Exception("Method "+methodName+" in class "+obj.getClass().getName()+" not found!");
    }
    
    /**
     * create AsyncExecutor instance
     * @param obj the object whose method is to be called
     * @param methodName the method name to be called
     * @param callbackMethodName the method to execute after the asynchronous call is completed
     * @param arguments the arguments to be passed to the method
     * @throws Exception thrown when one of the methods don't exist in the object
     */
    public AsyncExecutor(Object obj, String methodName, String callbackMethodName, Object... arguments) throws Exception {
        super();
        instance = obj;
        this.methodName = methodName;
        args = arguments;
        this.callbackMethodName = callbackMethodName;
        
        if(!isMethodExists(methodName))
        throw new Exception("Method "+methodName+" in class "+obj.getClass().getName()+" not found!");
        if(!isMethodExists(callbackMethodName))
        throw new Exception("Method "+callbackMethodName+" in class "+obj.getClass().getName()+" not found!");
    }

    /**
     * Asynchronously executes a method of an object
     * @param obj the object whose method is to be called
     * @param methodName the method name to be called
     * @param arguments the arguments to be passed to the method
     * @return this a Future object that holds the result after the async call is complete
     * @throws Exception thrown when one of the methods don't exist in the object
     */
    public static Future<Serializable> execute(Object obj, String methodName, Object... arguments) throws Exception {

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Serializable> result = exec.submit((Callable)new AsyncExecutor(obj, methodName, arguments));
        return result;
    }
    
    /**
     * Asynchronously executes a method of an object
     * @param obj the object whose method is to be called
     * @param methodName the method name to be called
     * @param callbackMethodName the method to execute after the asynchronous call is completed
     * @param arguments the arguments to be passed to the method
     * @return this a Future object that holds the result after the async call is complete
     */
    public static void executeWithCallback(Object obj, String methodName, String callbackMethodName, Object... arguments) throws Exception {

        ExecutorService exec = Executors.newSingleThreadExecutor();
        AsyncExecutor executor = new AsyncExecutor(obj, methodName,callbackMethodName, arguments);
        exec.execute((Runnable) executor);
    }


    /**
     * @inheritDoc
     */
    @Override
    public Serializable call() throws Exception {
        Class currentClass = instance.getClass();
        Method[] methods = currentClass.getDeclaredMethods();
        Method currentMethod = null;
        for(int i = 0;i<methods.length;i++) {
            if(methods[i].getName().equals(methodName))
            {
                currentMethod = methods[i];
                break;
            }
        }
        if(currentMethod == null)
            throw new Exception("Method "+methodName+" in class "+currentClass.getName()+" not found!");
        Serializable result = (Serializable)currentMethod.invoke(instance, args);
        return (Serializable)result;
    }
    
    public boolean isMethodExists(String methodName) {
        Class currentClass = instance.getClass();
        Method[] methods = currentClass.getDeclaredMethods();
        Method currentMethod = null;
        for(int i = 0;i<methods.length;i++) {
            if(methods[i].getName().equals(methodName))
            {
                currentMethod = methods[i];
                break;
            }
        }
        return currentMethod != null;
    }

    /**
     * This method invokes the method passed in the parameters and then invokes the callback method
     */
    @Override
    public void run() {
        Class currentClass = instance.getClass();
        Method[] methods = currentClass.getDeclaredMethods();
        Method currentMethod = null;
        for(int i = 0;i<methods.length;i++) {
            if(methods[i].getName().equals(methodName))
            {
                currentMethod = methods[i];
                break;
            }
        }
        /*if(currentMethod == null)
            throw new Exception("Method "+methodName+" in class "+currentClass.getName()+" not found!");*/
        Serializable result = null;
        try {
            result = (Serializable) currentMethod.invoke(instance, args);
        } catch (InvocationTargetException e) {
        } catch (IllegalAccessException e) {
        }
        Object[] callbackParam = new Object[1];
        callbackParam[0] = result;

        Method callbackMethod = null;
        for(int i = 0;i<methods.length;i++) {
            if(methods[i].getName().equals(callbackMethodName))
            {
                callbackMethod = methods[i];
                break;
            }
        }
        try {
            callbackMethod.invoke(instance, callbackParam);
        } catch (InvocationTargetException e) {
        } catch (IllegalAccessException e) {
        }

    }
}
