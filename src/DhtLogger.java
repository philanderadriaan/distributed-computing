package tcss558.team05;

import java.io.File;

import java.io.FileWriter;
import java.io.IOException;

import java.lang.management.ManagementFactory;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Used to print and log everything that happens in the system
 */
public class DhtLogger {
    /**
     * logger object
     */
    static Logger dhtLogger = getLogger();

    /**
     * Add the msg to the log and print it on the screen
     * @param msg the message to be logged
     */
    public synchronized static void log(String msg) 
    {
        dhtLogger.info(msg);
    }

    /**
     * This is called on the first call to this class to initialize the dhtLogger variable
     * @return a Logger that can write/append to the "dht.log" file in the current directory. It creates one if it doesn't exist
     */
    public synchronized static Logger getLogger() {
        Logger logger = Logger.getLogger("DhtLogger");  
                FileHandler handler;  
                  
                try {  
                      
                    // This block configure the logger with handler and formatter  
                    //String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
                    handler = new FileHandler("dht.log");
                    logger.addHandler(handler);  
                    //logger.setLevel(Level.ALL);  
                    handler.setFormatter(new DhtLogFormatter());  
                      
                } catch (SecurityException e) {  
                    e.printStackTrace();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
                return logger;
    }
}
