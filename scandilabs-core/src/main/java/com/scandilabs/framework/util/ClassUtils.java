package com.scandilabs.framework.util;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for dealing with java Class objects
 * 
 * @author mkvalsvik
 * 
 */
@SuppressWarnings(value={"rawtypes"})
public class ClassUtils {

    private static Logger log = LoggerFactory.getLogger(ClassUtils.class);

    /**
     * Helper method for running a junit test class outside a Junit test runner
     * (typically from a main() method)
     * 
     * @param clazz
     * @throws Exception
     */
    public static void runAsJunitTest(Class<?> clazz) throws Exception {
        Method setUp = null;
        try {
            setUp = clazz.getDeclaredMethod("setUp", new Class[] {});
        } catch (NoSuchMethodException e) {
            // ignore, we won't run the setup
        }
        Method tearDown = null;
        try {
            tearDown = clazz.getDeclaredMethod("tearDown", new Class[] {});
        } catch (NoSuchMethodException e) {
            // ignore, we won't run the tearDown
        }
        Method[] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().startsWith("test")) {
                Object test = clazz.newInstance();
                if (setUp != null) {
                    setUp.invoke(test, new Object[] {});
                }
                methods[i].invoke(test, new Object[] {});
                if (tearDown != null) {
                    tearDown.invoke(test, new Object[] {});
                }
            }
        }
    }

    /**
     * Compares two classes for equality
     * 
     * @param class1
     * @param class2
     * @return true if identical, false otherwise
     */
    public static boolean equals(Class<?> class1, Class<?> class2) {
        String class1Name = class1.getName();
        String class2Name = class2.getName();
        if (class1Name.equals(class2Name)) {
            return true;
        }

        return false;
    }

    /**
     * Try to find a resource, trying both current and context class loaders
     * 
     * @param callingClass
     * @param resourceName
     * @return a <code>URL</code> reference to the named resource or null if not
     *         found
     */
    public static URL getResource(Class<?> callingClass, String resourceName) {
        ClassLoader currentLoader = callingClass.getClassLoader();
        URL url = currentLoader.getResource(resourceName);
        if (url == null) {
            ClassLoader contextLoader = Thread.currentThread()
                    .getContextClassLoader();
            url = contextLoader.getResource(resourceName);
        }
        return url;
    }

    /**
     * Try to find a resource as an InputStream, trying both current and context
     * class loaders
     * 
     * @param callingClass
     * @param resourceName
     * @return an <code>InputStream</code> reference to the named resource
     */
    public static InputStream getResourceAsStream(Class<?> callingClass,
            String resourceName) {
        ClassLoader currentLoader = callingClass.getClassLoader();
        InputStream in = currentLoader.getResourceAsStream(resourceName);
        if (in == null) {
            ClassLoader contextLoader = Thread.currentThread()
                    .getContextClassLoader();
            in = contextLoader.getResourceAsStream(resourceName);
        }
        return in;
    }

    /**
     * Checks if the implementingClass is a subclass of the class or interface
     * represented by interfaceClass
     * 
     * @param implementingClass
     * @param interfaceClass
     * @return true if <code>implementingClassName</code> implements one or more
     *         of the <code>interfaceClasses</code>
     */
    public static boolean isInstanceOf(Class<?> implementingClass,
            Class<?> interfaceClass) {

        if (implementingClass == null && interfaceClass == null) {
            return true;
        }

        // Has no meaning if either class is null
        if (implementingClass == null) {
            log.warn("Parameter implementingClass was null");
            return false;
        }
        if (interfaceClass == null) {
            log.warn("Parameter interfaceClass was null");
            return false;
        }

        String interfaceClassName = interfaceClass.getName();
        String implementingClassName = implementingClass.getName();

        // Same class?
        if (interfaceClassName.equals(implementingClassName)) {
            return true;
        }

        // Recursively check all implemented interfaces
        for (int i = 0; i < implementingClass.getInterfaces().length; i++) {
            implementingClassName = implementingClass.getInterfaces()[i]
                    .getName();
            if (interfaceClassName.equals(implementingClassName)) {
                return true;
            }
            if (isInstanceOf(implementingClass.getInterfaces()[i],
                    interfaceClass)) {
                return true;
            }
        }

        // Recursively check the superclass
        Class<?> superClass = implementingClass.getSuperclass();
        if (superClass != null) {

            // we have a superclass which is not Object
            if (interfaceClassName.equals(superClass.getName())) {
                return true;
            }
            if (isInstanceOf(superClass, interfaceClass)) {
                return true;
            }
        }

        // By now we haven't found any superclasses or implemented interfaces.
        // Return false.
        return false;
    }

    /**
     * Check if implementing class is a subclass of any of the interfaceClasses
     * 
     * @param implementingClass
     *            the name of the class you want to test
     * @param interfaceClasses
     *            the classes or interfaces you want to test against (for
     *            inheritance from)
     * @return true if <code>implementingClassName</code> implements one or more
     *         of the <code>interfaceClasses</code>
     */
    public static boolean isInstanceOf(Class<?> implementingClass,
            Collection<?> interfaceClasses) {
        return isInstanceOf(implementingClass, interfaceClasses, false);
    }

    /**
     * Check if implementing class is a subclass of any of the interfaceClasses
     * 
     * @param implementingClass
     *            the name of the class you want to test
     * @param interfaceClasses
     *            the classes or interfaces you want to test against (for
     *            inheritance from)
     * @param ignoreImplementingClass
     *            if true, an implementingClass that is also included in the
     *            interfaceClasses will be ignored (i.e. will not return true)
     * @return true if <code>implementingClassName</code> implements one or more
     *         of the <code>interfaceClasses</code>
     */
    public static boolean isInstanceOf(Class<?> implementingClass,
            Collection<?> interfaceClasses, boolean ignoreImplementingClass) {
        Iterator<?> it = interfaceClasses.iterator();
        while (it.hasNext()) {
            Object obj = it.next();
            if (obj instanceof String) {
                if (isInstanceOf(implementingClass, (String) obj)) {
                    if (ignoreImplementingClass
                            && implementingClass.getName().equals(obj)) {
                        // skip
                    } else {
                        return true;
                    }
                }
            } else if (obj instanceof Class) {
                if (isInstanceOf(implementingClass, (Class<?>) obj)) {
                    Class<?> clazz = (Class<?>) obj;
                    if (ignoreImplementingClass
                            && implementingClass.getName().equals(
                                    clazz.getName())) {
                        // skip
                    } else {
                        return true;
                    }
                }
            } else {
                throw new IllegalArgumentException(
                        "Interface data type not supported: " + obj.getClass());
            }
        }
        return false;
    }

    /**
     * Checks if the given class is a subclass of the class or interface
     * represented by interfaceName
     * 
     * @param clazz
     *            the class to check
     * @param interfaceName
     *            the full name of the class or interface to check against
     * @return true if <code>clazz</code> implements <code>interfaceName</code>
     */
    public static boolean isInstanceOf(Class<?> clazz, String interfaceName) {
        Class<?> permInt = toClass(interfaceName);
        return isInstanceOf(clazz, permInt);
    }

    /**
     * Checks if the given class is a subclass of the class or interface
     * represented by interfaceName
     * 
     * @param className
     *            the full name of class to check
     * @param interfaze
     *            the interface to check against
     * @return true if <code>clazz</code> implements <code>interfaceName</code>
     */
    public static boolean isInstanceOf(String className, Class<?> interfaze) {
        Class<?> clazz = toClass(className);
        return isInstanceOf(clazz, interfaze);
    }

    /**
     * Check if implementing class is a subclass of any of the interfaceClasses
     * 
     * @param implementingClassName
     *            the name of the class you want to test
     * @param interfaceClasses
     *            the classes or interfaces you want to test against (for
     *            inheritance from) true if <code>implementingClassName</code>
     *            implements one or more of the <code>interfaceClasses</code>
     * @return true if <code>implementingClassName</code> implements one or more
     *         of the <code>interfaceClasses</code>
     */
    public static boolean isInstanceOf(String implementingClassName,
            Collection<?> interfaceClasses) {
        try {
            Class<?> implementingClass = Class.forName(implementingClassName);
            return isInstanceOf(implementingClass, interfaceClasses);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found: "
                    + implementingClassName, e);
        }
    }

    /**
     * Checks if the given class is a subclass of the class or interface
     * represented by interfaceName
     * 
     * @param className
     *            the full name of class to check
     * @param interfaceName
     *            the full name of the class or interface to check against
     * @return true if <code>clazz</code> implements <code>interfaceName</code>
     */
    public static boolean isInstanceOf(String className, String interfaceName) {
        Class<?> clazz = toClass(className);
        Class<?> permInt = toClass(interfaceName);
        return isInstanceOf(clazz, permInt);
    }

    /**
     * Convenience method for creating a new instance without having to catch
     * checked exceptions
     * 
     * @param clazz
     *            the Class for which an instance should be created
     * @return the newly instantiated object
     * @throws IllegalParameterException
     *             if the class can't be found or accessed
     */
    public static Object newInstance(Class<?> clazz) {
        Object obj = null;
        try {
            obj = clazz.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                    "Could not instantiate instance of " + clazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Not allowed to instantiate instance of " + clazz.getName(),
                    e);
        }
        return obj;
    }

    /**
     * Convenience method for creating a new instance without having to catch
     * checked exceptions
     * 
     * @param className
     *            the full class name
     * @return the newly instantiated object
     * @throws IllegalParameterException
     *             if the class can't be found or accessed
     */
    public static Object newInstance(String className) {
        Class<?> clazz = toClass(className);
        return newInstance(clazz);
    }

    /**
     * Convert a class name to a loaded <code>Class</code> object
     * 
     * @param className
     * @return the loaded <code>Class</code>
     */
    public static Class<?> toClass(String className) {
        String trimmedClassName = className.trim();
        ClassLoader currentLoader = ClassUtils.class.getClassLoader();
        Class<?> clazz = null;
        try {
            clazz = Class.forName(trimmedClassName, true, currentLoader);
        } catch (ClassNotFoundException e) {
            ClassLoader contextLoader = Thread.currentThread()
                    .getContextClassLoader();
            if (contextLoader != null) {
                try {
                    clazz = Class
                            .forName(trimmedClassName, true, contextLoader);
                } catch (ClassNotFoundException e2) {
                    throw new IllegalArgumentException(
                            "Class not found using contextClassLoader "
                                    + contextLoader
                                    + " or current classLoader "
                                    + currentLoader + ": " + trimmedClassName,
                            e2);
                }
            }
        }
        return clazz;
    }

    /**
     * Converts a collection of class names (Strings) to an array of Class
     * objects
     * 
     * @param classNames
     *            a collection of Strings or Classes
     * @return the array of Class objects
     */
    public static Class[] toClassArray(Collection<?> classNames) {
        Class[] classes = new Class[classNames.size()];
        int i = 0;
        for (Iterator<?> iter = classNames.iterator(); iter.hasNext();) {
            Object obj = iter.next();
            if (obj instanceof String) {
                classes[i] = toClass((String) obj);
            } else if (obj instanceof Class) {
                classes[i] = (Class<?>) obj;
            } else {
                throw new IllegalArgumentException("Can't handle type: "
                        + obj.getClass().getName());
            }

            i++;
        }
        return classes;
    }

    /**
     * Converts a String array of class names to an array of Class objects
     * 
     * @param classNames
     *            an array of Strings
     * @return the array of Class objects
     */
    public static Class[] toClassArray(String[] classNames) {
        Class[] classes = new Class[classNames.length];
        for (int i = 0; i < classNames.length; i++) {
            classes[i] = toClass(classNames[i]);
        }
        return classes;
    }

    /**
     * Transform a full classname with package info (i.e. 'java.lang.Object')
     * into a short classname without package (i.e. 'Object')
     * 
     * @param clazz
     *            the class to inspect
     * @return String
     */
    public static String toShortName(Class<? extends Object> clazz) {
        if (clazz != null) {
            return toShortName(clazz.getName());
        }
        return null;
    }

    /**
     * Transform a full classname with package info (i.e. 'java.lang.Object')
     * into a short classname without package (i.e. 'Object')
     * 
     * @param obj
     *            the object whose class to convert
     * @return String
     */
    public static String toShortName(Object obj) {
        if (obj == null) {
            return null;
        }
        return toShortName(obj.getClass());
    }

    /**
     * Transform a full classname with package info (i.e. 'java.lang.Object')
     * into a short classname without package (i.e. 'Object')
     * 
     * @param longClassName
     * @return String
     */
    public static String toShortName(String longClassName) {
        int lastDotPos = longClassName.lastIndexOf('.');
        if (lastDotPos != -1) {
            return longClassName.substring(lastDotPos + 1);
        }
        return longClassName;
    }

    private ClassUtils() {
    }
}
