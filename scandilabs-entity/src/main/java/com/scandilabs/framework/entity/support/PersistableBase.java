package com.scandilabs.framework.entity.support;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

import org.catamarancode.util.ClassUtils;
import org.catamarancode.util.Timestamped;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.metadata.ClassMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for all persistent entity objects in an application. Provides
 * extending objects with defaults behaviors that make it easier to implement a
 * Domain-Driven Development paradigm with hibernate.
 * 
 * test comment 2nd test comment
 * 
 * @author mkvalsvik
 * 
 */
@MappedSuperclass
public abstract class PersistableBase extends IdentifiableBase implements
		Persistable {

	private static Logger logger = LoggerFactory
			.getLogger(PersistableBase.class);

	private long id;

	/**
	 * Allows access to outside services from persistent domain objects. Must be
	 * statically injected.
	 */
	private static Map<String, Object> entityServices;

	/**
	 * Allows internal access to hibernate session factory. Must be statically
	 * injected.
	 */
	private static SessionFactory sessionFactory;

	@Id
	@GeneratedValue
	public Long getId() {
		return id;
	}

	@Transient
	protected static Map<String, Object> getEntityServices() {
		return entityServices;
	}

	/**
	 * Saves the entity using the injected sessionFactory.
	 * 
	 * Note that we are using SessionFactoryUtils.getSession(..) to retrieve the
	 * hibernate session bound to threadlocal instead of relying on Spring's
	 * HibernateTemplate to do it. Either way probably works but using
	 * SessionFactoryUtils allows for greater control of session binding and
	 * release, which was found to be useful when setting up unit tests that
	 * include lazy loading of objects (in assert statements).
	 * 
	 * @return the id of the saved entity
	 */
	public long save() {
		Session session = PersistableUtils.getHibernateSession(sessionFactory);
		// logger.warn("FLUSHMODE: " + session.getFlushMode());
		if (this instanceof Timestamped) {
			((Timestamped) this).setLastModifiedTime(new Date());
		}
		if (this.getId().longValue() == 0) {
			if (this instanceof Timestamped) {
				((Timestamped) this).setCreatedTime(new Date());
			}
			session.save(this);
		} else {

			// Only call explicit update if instanced is not known to the
			// session (o/w it will be updated at flush time)
			if (!session.contains(this)) {
				session.update(this);
			}
		}

		// Need to manually flush, as we've removed AUTO_FLUSH from the
		// OpenSessionInViewFilter
		session.flush();

		return this.getId();
	}

	@SuppressWarnings("unused")
	public void setId(Long id) {
		this.id = id;
	}

	@Transient
	public static void setEntityServices(Map<String, Object> services) {
		entityServices = services;
	}

	@Transient
	public static void setSessionFactory(SessionFactory s) {
		sessionFactory = s;
		
		// Initialize persistent entities to work with the "Person.objects.all()" EntityFinder syntax
		for (String name : sessionFactory.getAllClassMetadata().keySet()) {
			ClassMetadata meta = sessionFactory.getAllClassMetadata().get(name);
			Class clazz = ClassUtils.toClass(meta.getEntityName());
			try {
				Field field = clazz.getField("objects");
				field.set(null, new EntityFinder(s, clazz));
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException("Catamaran Framework:  You need to add 'public static EntityFinder objects;' to entity class " + clazz.getName(), e);
			} catch (IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			logger.info("Initialized field 'objects' with EntityFinder for: " + meta.getEntityName());
		}
	}

	public static EntityFinder objects(Class clazz) {
		
        EntityFinder finder = entityFinders.get(clazz.getName());
        if (finder == null) {
        	finder = new EntityFinder(sessionFactory, clazz);
        	entityFinders.put(clazz.getName(), finder);
        }
        
        return finder;
	}
	
	/**
	 * Cached entity finders
	 */
	private static Map<String, EntityFinder> entityFinders = new HashMap<String, EntityFinder>();

	/**
	 * Create a subset of an object's properties that together comprise the object's natural key: http://en.wikipedia.org/wiki/Natural_key
	 * 
	 * Example: A Person class might use firstName, lastName, street and zip to
	 * uniquely identify an individual.
	 * 
	 * This default implementation will use all object properties of class String.  It is recommended that extending classes subclass this method.
	 * @return
	 */
	public Map<String, Object> naturalKey() {
		Map<String, Object> naturalKeyProperties = new HashMap<String, Object>();
		
		try {
			for(PropertyDescriptor propertyDescriptor : 
			    Introspector.getBeanInfo(this.getClass()).getPropertyDescriptors()) {
			
				Method getter = propertyDescriptor.getReadMethod();
				
				// Skip getters marked with @Transient because these are not persisted via JPA/Hibernate
				if (getter.getAnnotation(Transient.class) != null) {
					continue;
				}
				
				if (getter != null && ClassUtils.isInstanceOf(getter.getReturnType(), String.class)) {
					Object value = getter.invoke(this, null);
					String propertyName = getter.getName().substring(3, 4).toLowerCase() + getter.getName().substring(4);
					logger.trace("Adding to natural key: property " + propertyName);
					naturalKeyProperties.put(propertyName, value);
				}
			}
		} catch (IntrospectionException e1) {
			throw new RuntimeException(e1);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);			
		}
		return naturalKeyProperties;
	}
	
    /**
     * Purges this entity from the system. This is a hard delete
     */
    public void delete() {
        Session session = PersistableUtils.getHibernateSession(sessionFactory);
        session.delete(this);
        // Need to manually flush, as we've removed AUTO_FLUSH from the OpenSessionInViewFilter
        session.flush();
    }

	/**
	 * Uses properties of current object to query database for a persistent
	 * entity that matches the current (assumed to be detatched) object.
	 * 
	 * @return
	 */
	public Persistable locateEntity() {
		List<? extends Persistable> list = PersistableUtils.filter(
				sessionFactory, this.getClass(), this.naturalKey());
		if (list.size() > 1) {
			throw new NotUniqueResultException(
					String.format(
							"Not unique result: Query on class %s using %d parameter arguments resulted in %d matches",
							this.getClass().getName(), this.naturalKey().size(), list.size()));
		}

		if (list.isEmpty()) {
			return null;
		}
		return list.iterator().next();
	}

}
