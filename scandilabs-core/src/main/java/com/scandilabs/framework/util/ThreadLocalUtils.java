package com.scandilabs.framework.util;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpSession;

/**
 * A local utility class to retrieve {@link ThreadLocal} variables.
 * e.g.:
 * Every Controllers are responsible to set the {@link HttpSession} on the {@link ThreadLocal} instance
 * 
 * This class may go off if the session object is passed at a 2-3 level depth of method chain invocation
 */
public class ThreadLocalUtils {

	//private threadLocal 
	private static ThreadLocal<Map<String, Object>> threadLocal = new ThreadLocal<Map<String, Object>>();
	
	//private static Map<String, Object> mapOfObjects = new HashMap<String, Object>();
	
	/**
	 * Set the <key,value> on the {@link ThreadLocal} instance
	 * @param key
	 * @param value
	 */
	public static void set(String key, Object value) {

	    Map<String, Object> mapOfObjects = null;
	    
		//this is the firstTime for this Thread!
		if(threadLocal.get()==null){
		    mapOfObjects = new HashMap<String, Object>();
			threadLocal.set(mapOfObjects);
		}
		
		((Map<String, Object>)threadLocal.get()).put(key, value);
	}
	
	/**
	 * Get the value of the key set from the {@link ThreadLocal} instance
	 * @param key
	 * @return
	 */
	public static Object get(String key) {
		if(threadLocal.get()==null){
			return null;
		}
		
		return ((Map<String, Object>)threadLocal.get()).get(key);
	}
	
	/**
	 * Remove/Clear the value of the passed in key
	 * @param key
	 */
	public static void remove(String key) {
		if(((Map<String, Object>)threadLocal.get()).containsKey(key)){
			((Map<String, Object>)threadLocal.get()).remove(key);
		}
	}
	

}
