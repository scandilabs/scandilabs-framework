package com.scandilabs.framework.entity.support;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.catamarancode.util.ClassUtils;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class PersistableUtils {
	
	private static Logger logger = LoggerFactory
			.getLogger(PersistableUtils.class);
	
	private static StatelessSession statelessSession;

	
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
    public static Object loadPersistentEntity(SessionFactory sessionFactory, Class clazz, Long id) {
        if (!ClassUtils.isInstanceOf(clazz, PersistableBase.class)) {
            throw new RuntimeException(
                    "Class does not inherit from persistent entity base class: "
                            + clazz.getName());
        }
        Session session = getHibernateSession(sessionFactory);
        logger.debug("Got hibernate session object with hashcode: " + System.identityHashCode(session));
        Object obj = null;

        try {
            obj = session.load(clazz, id);
        } catch (org.hibernate.ObjectNotFoundException e) {
            throw new RepositoryObjectNotFoundException(e.getMessage(), e);
        }
        return obj;
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
    public static Object getPersistentEntity(SessionFactory sessionFactory, Class clazz, Long id) {
        if (!ClassUtils.isInstanceOf(clazz, PersistableBase.class)) {
            throw new RuntimeException(
                    "Class does not inherit from persistent entity base class: "
                            + clazz.getName());
        }
        Session session = getHibernateSession(sessionFactory);
        Object obj = session.get(clazz, id);
        return obj;
    }
    
    /**
     * Execute the passed in Query String and returns a result set.
     * @param queryString
     * @return
     */
    public static List<? extends Persistable> executeQuery(SessionFactory sessionFactory, String queryString) {
    	Session session = getHibernateSession(sessionFactory);
		Query query = session.createQuery(queryString);
		return query.list();
    }
    
    /**
     * Bind a session to ThreadLocal for use by this thread. Note Spring MVC
     * Framework will normally do this automatically. This method exists so it
     * can be used in contexts outside Spring (such as unit tests, command-line
     * java applications, etc)
     */
    public static void manuallyBindHibernateSessionToThreadLocal(SessionFactory sessionFactory) {
    	Session session = getHibernateSession(sessionFactory);
        /*
         * session.setFlushMode(FlushMode.MANUAL); logger.warn("FLUSHMODE: " +
         * session.getFlushMode());
         */
        TransactionSynchronizationManager.bindResource(sessionFactory,
                session);
    }
    
    /**
     * Centralized method for accessing current hibernate session
     * @param sessionFactory
     * @return
     */
    public static Session getHibernateSession(SessionFactory sessionFactory) {
    	Session session = sessionFactory.getCurrentSession();
    	logger.debug("Got hibernate session object with hashcode: " + System.identityHashCode(session));
    	return session;
    }

    /**
     * Unbind session from ThreadLocal and release session. Note Spring MVC
     * Framework will normally do this automatically. This method exists so it
     * can be used in contexts outside Spring (such as unit tests, command-line
     * java applications, etc)
     */
    public static void manuallyUnBindHibernateSessionFromThreadLocal(SessionFactory sessionFactory) {
    	Session session = getHibernateSession(sessionFactory);
        if (session != null) {
            TransactionSynchronizationManager
                    .unbindResource(sessionFactory);
            session.close();
        }
    }

    /**
     * Flush the current hibernate session (issue queries to update database
     * with changes). Note: Consider moving flush to the end of
     * OpenSessionInView to speed things up and reduce number of flushes
     */
    public static void flush(SessionFactory sessionFactory) {
    	Session session = getHibernateSession(sessionFactory);
        session.flush();
    }
    
	/**
	 * Retrieves all persistent objects of a given type
	 * 
	 * @return
	 */
	public static List<? extends Persistable> all(SessionFactory sessionFactory, Class clazz) {
		Session session = getHibernateSession(sessionFactory);
		Criteria crit = session.createCriteria(clazz);
		return Collections.checkedList(crit.list(), Persistable.class);
	}

	/**
	 * Retrieves a list of persistent objects that matches the given filter
	 * parameters
	 * 
	 * @param queryParameters
	 * @return
	 */
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Map<String, Object> queryParameters) {
		Session session = getHibernateSession(sessionFactory);
		Criteria crit = session.createCriteria(clazz);
		for (String name : queryParameters.keySet()) {
			Object value = queryParameters.get(name);
			// TODO: Remove restriction to only string
			if (value != null && value instanceof String) {
				crit.add(Restrictions.eq(name, value));	
			}			
		}
		return crit.list();
	}
	
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Set<Criterion> criteria) {
		return filter(sessionFactory, clazz, criteria, Collections.EMPTY_LIST);
	}
	
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Criterion criterion) {
		Set<Criterion> criteria = new HashSet<Criterion>();
		criteria.add(criterion);
		return filter(sessionFactory, clazz, criteria, Collections.EMPTY_LIST);
	}
	
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Set<Criterion> criteria, Order order) {
		List<Order> orders = new ArrayList<Order>();
		orders.add(order);
		return filter(sessionFactory, clazz, criteria, orders);
	}
	
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Criterion criterion, Order order) {
		List<Order> orders = new ArrayList<Order>();
		orders.add(order);
		Set<Criterion> criteria = new HashSet<Criterion>();
		criteria.add(criterion);
		return filter(sessionFactory, clazz, criteria, orders);
	}
	
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Criterion criterion, List<Order> orders) {
		Set<Criterion> criteria = new HashSet<Criterion>();
		criteria.add(criterion);
		return filter(sessionFactory, clazz, criteria, orders);
	}
	
	public static List<? extends Persistable> order(SessionFactory sessionFactory, Class clazz, 
			List<Order> orders) {		
		return filter(sessionFactory, clazz, Collections.EMPTY_SET, orders);
	}

	public static List<? extends Persistable> order(SessionFactory sessionFactory, Class clazz, 
			Order order) {		
		List<Order> orders = new ArrayList<Order>();
		orders.add(order);
		return order(sessionFactory, clazz, orders);
	}
	
	public static List<? extends Persistable> filter(SessionFactory sessionFactory, Class clazz, 
			Set<Criterion> criteria, List<Order> orders) {
		Session session = getHibernateSession(sessionFactory);
		Criteria crit = session.createCriteria(clazz);
		for (Criterion criterion : criteria) {
			crit.add(criterion);
		}
		for (Order order : orders) {
			crit.addOrder(order);
		}
		return crit.list();
	}
	
	
    

}
