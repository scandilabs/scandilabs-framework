package com.scandilabs.framework.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;

import org.apache.commons.beanutils.PropertyUtils;

/**
 * Compares a given property (sortPropertyName, set in constructor) on two beans
 * (which must both have a getter for the sort property)
 * 
 * @author mkvalsvik
 * 
 */
@SuppressWarnings(value={"rawtypes","unchecked"})
public class BeanPropertyComparator implements Comparator {

	public BeanPropertyComparator(String sortPropertyName) {
		this.sortPropertyName = sortPropertyName;
	}

	private String sortPropertyName;

	public int compare(Object o1, Object o2) {
		Object property1 = null;
		Object property2 = null;
		try {
			property1 = PropertyUtils.getProperty(o1, this.sortPropertyName);
			property2 = PropertyUtils.getProperty(o2, this.sortPropertyName);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Exception invoking getter " + sortPropertyName + ": " + e.toString());
		} catch (InvocationTargetException e) {
			throw new RuntimeException("Exception invoking getter " + sortPropertyName + ": " + e.toString());
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Exception invoking getter " + sortPropertyName + ": " + e.toString());
		}
		return org.apache.commons.collections.ComparatorUtils.NATURAL_COMPARATOR
				.compare(property1, property2);
	}

}
