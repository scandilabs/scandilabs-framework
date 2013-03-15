package org.catamarancode.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings(value={"rawtypes"})
public final class StringUtils {
	
	private StringUtils() {}
	
	public static String removeDuplicatePipeDelimitedTerms(String s) {
	    if (s == null) { 
	        return null;
	    }
	    if (s.indexOf("|") == -1) {
	        return s;
	    }
	    String[] terms = s.split("\\|");
	    TreeSet<String> termSet = new TreeSet<String>(); 
	    for (int i = 0; i < terms.length; i++) {
	        termSet.add(terms[i].trim());
	    }
	    
	    // back to string
	    StringBuilder sb = new StringBuilder();
	    Iterator iter = termSet.iterator();
	    while (iter.hasNext()) {
	        String term = (String) iter.next();
	        sb.append(term);
	        if (iter.hasNext()) {
	            sb.append(" | ");
	        }
	    }
	    return sb.toString();
	}
	
	/**
	 * Turns a camel-case string like ThisIsATest into a dash-separated string, like this-is-a-test
	 * @param stringList
	 * @return
	 */
	public static String camelCaseToDashSeparatedString(String s) {
		List<String> parts = splitOnCaps(s);
		return toDashSeparatedString(parts);
	}
	
	/**
	 * Turns a camel-case string like ThisIsATest into a list of strings like [this,is,a,test]
	 * @param s
	 * @return
	 */
	public static List<String> splitOnCaps(String s) {		
		List<String> results = new ArrayList<String>();
        Pattern p = Pattern.compile("[A-Z]{1}[a-z]*");
        Matcher m = p.matcher(s);
        while(m.find()){
        	results.add(m.group());
        }
        return results;
	}
	
	/**
	 * Turns a list of strings into a dash-separated string, like [this,is,a,test] to this-is-a-test
	 * @param stringList
	 * @return
	 */
	public static String toDashSeparatedString(List<String> stringList) {
		
		StringBuilder result = new StringBuilder();
		Iterator<String> iter = stringList.iterator();
		while (iter.hasNext()) {
			String s = iter.next();
			result.append(s.toLowerCase());
			if (iter.hasNext()) {
				result.append("-");
			}
		}
		return result.toString();
	}
	
	public static String classNameToDashSeparatedString(Class clazz) {
		List<String> parts = splitOnCaps(clazz.getSimpleName());
		parts.remove(parts.size() - 1); // remove "Controller"
		return toDashSeparatedString(parts);
	}
	
	/**
	 * Converts spaces to hyphens and then removes ant remaining special characters from the string
	 * (anything non alpha-numeric, except hypen, underscore, parentheses.)
	 * @param s
	 * @return
	 */
	public static String removeSpecialCharacters(String s) {
		// Change spaces to hyphens
        Pattern p = Pattern.compile("\\s");
        Matcher m = p.matcher(s);
        String hyphenated = m.replaceAll("-");
        
        // Remove other special characters
        p = Pattern.compile("[^()a-zA-Z0-9_-]");
        m = p.matcher(hyphenated);
        
        return m.replaceAll("");
	}


}
