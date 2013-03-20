package com.scandilabs.framework.entity.support;

import java.util.List;

import org.catamarancode.util.ClassUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate3.SessionFactoryUtils;
import org.springframework.orm.hibernate3.SessionHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * A base class for Repository classes that provides convenience methods to
 * extending repository classes. A Repository should encapsulate all logic
 * related to loading and querying for persistent entities.
 * 
 * @author mkvalsvik
 * 
 */
public class RepositoryBase {

    private static Logger logger = LoggerFactory
            .getLogger(RepositoryBase.class);

    private SessionFactory sessionFactory;

    public RepositoryBase(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Loads an object by given class and id. Wraps any exceptions in a
     * RepositoryObjectNotFoundException in order to reduce tight coupling with
     * Hibernate
     * 
     * @param clazz
     *            - the class of the desired object
     * @param id
     *            - the identifier of desired object
     * @throws RepositoryObjectNotFoundException
     *             if object not found
     */
    protected Object loadPersistentEntity(Class clazz, Long id) {
    	return PersistableUtils.loadPersistentEntity(sessionFactory, clazz, id);
    }

    /**
     * Gets an object by given class and id. Unlike load(), a get() will return
     * null if the object can't be found.
     * 
     * @param clazz
     *            - the class of the desired object
     * @param id
     *            - the identifier of desired object
     * @return the entity object identified by given id, or null if not found
     */
    protected Object getPersistentEntity(Class clazz, Long id) {
    	return PersistableUtils.getPersistentEntity(sessionFactory, clazz, id);
    }

    /**
     * Execute the passed in Query String and returns a result set.
     * @param queryString
     * @return
     */
    protected List<? extends Persistable> executeQuery(String queryString) {
    	return PersistableUtils.executeQuery(sessionFactory, queryString);
    }
    
    /**
     * Bind a session to ThreadLocal for use by this thread. Note Spring MVC
     * Framework will normally do this automatically. This method exists so it
     * can be used in contexts outside Spring (such as unit tests, command-line
     * java applications, etc)
     */
    public void manuallyBindHibernateSessionToThreadLocal() {
    	PersistableUtils.manuallyBindHibernateSessionToThreadLocal(sessionFactory);
    }

    /**
     * Unbind session from ThreadLocal and release session. Note Spring MVC
     * Framework will normally do this automatically. This method exists so it
     * can be used in contexts outside Spring (such as unit tests, command-line
     * java applications, etc)
     */
    public void manuallyUnBindHibernateSessionFromThreadLocal() {
    	PersistableUtils.manuallyUnBindHibernateSessionFromThreadLocal(sessionFactory);
    }

    /**
     * Flush the current hibernate session (issue queries to update database
     * with changes). Note: Consider moving flush to the end of
     * OpenSessionInView to speed things up and reduce number of flushes
     */
    public void flush() {
    	PersistableUtils.flush(sessionFactory);
    }

    /**
     * Exposes the Hibernate session factory directly.  Should not normally be needed.
     * @return
     */
    protected SessionFactory getSessionFactory() {
        return sessionFactory;
    }

}
