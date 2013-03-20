package com.scandilabs.framework.util;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Handy operations on Collections 
 * 
 * @author mkvalsvik
 */
@SuppressWarnings(value={"rawtypes","unchecked"})
public class CollectionUtils {

	private static Log log = LogFactory.getLog(CollectionUtils.class);

	/**
	 * Converts a list of string values to a map with the string value as both
	 * key and value
	 * 
	 * @param stringValues
	 * @return a Map
	 */
	public static Map toMap(List stringValues) {
		
		Map map = new HashMap();
		for (Iterator iter = stringValues.iterator(); iter.hasNext();) {
			String value = (String) iter.next();
			map.put(value, value);
		}
		return map;
	}

	public static List extractUniquePropertyList(List source,
			String propertyName) {
		List result = new ArrayList();
		Iterator it = source.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			try {
				Object value = PropertyUtils.getProperty(o, propertyName);
				if (!result.contains(value) && value != null
						&& !value.equals("")) {
					result.add(value);
				}
			} catch (IllegalAccessException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			} catch (InvocationTargetException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			} catch (NoSuchMethodException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			}
		}
		return result;
	}

	public static void sortListByProperty(List list, String propertyName) {
		BeanPropertyComparator comparator = new BeanPropertyComparator(
				propertyName);
		Collections.sort(list, comparator);
	}

	/**
	 * Check if the property values of an object match those property name/value
	 * pairs in criteria
	 * 
	 * @param o
	 * @param criteria
	 * @return
	 */
	public static boolean match(Object o, Map criteria) {
		Iterator it = criteria.keySet().iterator();
		while (it.hasNext()) {
			String name = (String) it.next();
			Object value = criteria.get(name);
			try {
				if (value == null
						|| !value.equals(PropertyUtils.getProperty(o, name))) {
					return false;
				}
			} catch (IllegalAccessException e) {
				log.warn("Exception invoking getter " + name + ": "
						+ e.toString());
			} catch (InvocationTargetException e) {
				log.warn("Exception invoking getter " + name + ": "
						+ e.toString());
			} catch (NoSuchMethodException e) {
				log.warn("Exception invoking getter " + name + ": "
						+ e.toString());
			}
		}
		return true;
	}

	public static List extractUniquePropertyList(List source,
			String propertyName, Map criteria) {
		List result = new ArrayList();
		Iterator it = source.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			try {
				Object value = PropertyUtils.getProperty(o, propertyName);
				if (!result.contains(value) && value != null
						&& !value.equals("")) {

					// Check against criteria
					if (match(o, criteria)) {
						result.add(value);
					}
				}
			} catch (IllegalAccessException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			} catch (InvocationTargetException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			} catch (NoSuchMethodException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			}
		}
		return result;
	}

	public static List extractStringList(List source, String propertyName) {
		List result = new ArrayList();
		Iterator it = source.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			try {
				String value = BeanUtils.getProperty(o, propertyName);
				if (!result.contains(value) && value != null
						&& !value.equals("")) {
					result.add(value);
				}
			} catch (IllegalAccessException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			} catch (InvocationTargetException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			} catch (NoSuchMethodException e) {
				log.warn("Exception invoking getter " + propertyName + ": "
						+ e.toString());
			}
		}
		return result;
	}

	public static List mapToEntryList(Map map) {
		return mapToEntryList(map, true);
	}

	/**
	 * Build a List from the entry set of a map NOTE: This should not be
	 * necessary but there are bugs when you iterate using
	 * map.entrySet().iterator()
	 * 
	 * @param map
	 * @param sorted
	 *            - sorty by value (entry)
	 * @return
	 */
	public static List mapToEntryList(Map map, boolean sortedByValue) {
		List retList = new ArrayList();
		if (map.isEmpty()) {
			return Collections.EMPTY_LIST;
		}
		Iterator it = map.keySet().iterator();
		while (it.hasNext()) {
			Object key = it.next();
			Object obj = map.get(key);
			retList.add(obj);
		}

		// Sort
		if (sortedByValue)
			Collections.sort(retList);
		return retList;
	}

	/**
	 * Build a List from the entry set of a map NOTE: This should not be
	 * necessary but there are bugs when you iterate using
	 * map.entrySet().iterator()
	 * 
	 * @param map
	 * @param sorted
	 *            - sorty by keys
	 * @return
	 */
	public static List mapToEntryListSortedByKeys(Map map) {
		List retList = new ArrayList();
		if (map.isEmpty()) {
			return Collections.EMPTY_LIST;
		}
		List keyList = new ArrayList(map.keySet());
		Collections.sort(keyList);
		Iterator it = keyList.iterator();
		while (it.hasNext()) {
			Object key = it.next();
			Object obj = map.get(key);
			retList.add(obj);
		}

		return retList;
	}

	/**
	 * Convert a collection of beans to a collection of maps
	 * 
	 * @param beans
	 * @return
	 */
	public Collection beansToMaps(Collection beans) {
		List maps = new ArrayList();
		Iterator it = beans.iterator();
		while (it.hasNext()) {
			Object row = it.next();
			Map propMap = beanToMap(row);
			maps.add(propMap);
		}
		return maps;
	}

	public static Map beanToMap(Object bean) {
		Map propMap = null;
		try {
			propMap = PropertyUtils.describe(bean);
		} catch (Exception e) {
			throw new RuntimeException("Error when converting bean to map", e);
		}
		return propMap;
	}

	public static List mapToKeyList(Map map) {
		List retList = new ArrayList();
		if (!map.isEmpty()) {
			Iterator it = map.keySet().iterator();
			while (it.hasNext()) {
				retList.add(it.next());
			}
			return retList;
		} else {
			return Collections.EMPTY_LIST;
		}
	}

	/**
	 * Build a List from the key set of a map
	 * 
	 * @param map
	 * @return
	 */
	public static List mapToKeyList(BidiMap map, boolean sortedByValues) {
		if (!sortedByValues) {
			return mapToKeyList(map);
		}

		if (map.isEmpty()) {
			return Collections.EMPTY_LIST;
		}

		// Order by values
		Map inverseMap = map.inverseBidiMap();
		List values = mapToEntryList(map, true);
		List retList = new ArrayList();
		Iterator it = values.iterator();
		while (it.hasNext()) {
			retList.add(inverseMap.get(it.next()));
		}
		return retList;
	}

	public static Object findFirst(Collection coll) {
		if (coll.isEmpty()) {
			return null;
		} else {
			return coll.iterator().next();
		}
	}

	public static Object findOne(Collection coll) {
		if (coll.isEmpty()) {
			return null;
		} else if (coll.size() > 1) {
			throw new RuntimeException(
					"Expected only one member in collection, found many: "
							+ coll.toString());
		} else {
			return coll.iterator().next();
		}
	}

	/**
	 * Search a collection for a PersistentObject by the given ID
	 * 
	 * @param coll
	 * @param id
	 * @return
	 * 
	 *         public static PersistentObject findFirstByID(Collection coll,
	 *         String id) { PersistentObject po = null; Iterator it =
	 *         coll.iterator(); while (it.hasNext()) { po = (PersistentObject)
	 *         it.next(); if (id.equals(po.getId())) { return po; } } return
	 *         null; }
	 */

	/**
	 * Subtract objects in b from collection a. Uses the string value of the
	 * objects for comparison
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static Collection subtractByString(Collection a, Collection b) {
		Collection retColl = new ArrayList();
		Object obj = null;
		Iterator it = a.iterator();
		while (it.hasNext()) {
			obj = it.next();
			if (!b.contains(obj)) {
				retColl.add(obj);
			}
		}
		return retColl;
	}

	/**
	 * Subtract objects in b from collection a. Uses the id property of
	 * PersistentObjects for comparison.
	 * 
	 * @param a
	 * @param b
	 * @return
	 * 
	 *         public static Collection subtractByID(Collection a, Collection b)
	 *         { Collection retColl = new ArrayList(); PersistentObject po =
	 *         null; Iterator it = a.iterator(); while (it.hasNext()) { po =
	 *         (PersistentObject) it.next(); if (findFirstByID(b, po.getId()) ==
	 *         null) { retColl.add(po); } } return retColl; }
	 */

	public static String toString(Collection coll) {
		return toString(coll, ", ");
	}

	public static String toString(Collection coll, String delim) {
		StringBuffer sb = new StringBuffer();
		for (Iterator it = coll.iterator(); it.hasNext();) {
			Object obj = it.next();
			sb.append(String.valueOf(obj));
			if (it.hasNext()) {
				sb.append(delim);
			}
		}
		return sb.toString();
	}

}
