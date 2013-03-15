package org.catamarancode.util;

import java.util.Collection;
import java.util.Iterator;

/**
 * Utilities related to arrays
 * 
 * @see http://commons.apache.org/lang/api-2.5/org/apache/commons/lang/ArrayUtils.html 
 */
public class ArrayUtils {
    
    private ArrayUtils() {}

    public static boolean contains(String[] array, String valueToFind, boolean ignoreCase) {
        for (int i = 0; i < array.length; i++) {
            if (ignoreCase) {
                if (array[i].equalsIgnoreCase(valueToFind)) {
                    return true;
                }
            } else {
                if (array[i].equals(valueToFind)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Converts an array into a character-delimited string
     * @param arr
     * @param delim
     * @return
     */
    public static String toString(Object[] arr, String delim) {
        if (arr == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < arr.length; i++) {
            Object obj = arr[i];
            sb.append(String.valueOf(obj));
            if (i < (arr.length - 1)) {
                sb.append(delim);
            }
        }
        return sb.toString();
    }
    
}
