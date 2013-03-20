package com.scandilabs.framework.entity.support;

import java.util.Map;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.InitializingBean;

/**
 * A configuration-time object that allows for spring configuration of
 * application services that are then statically injected into the parent class
 * of entity objects. This allows for Domain-Driven Development where more logic
 * may reside in entity classes.
 * 
 * @author mkvalsvik
 * 
 */
public class EntityServicesContext implements InitializingBean {

    private Map<String, Object> entityServices;
    private SessionFactory sessionFactory;

    public void afterPropertiesSet() throws Exception {

        // Initialize the base object for all domain classes with entity
        // services context so that the persistent objects have easy access to
        // services
        PersistableBase.setEntityServices(this.entityServices);

        // Used by save() in PersistableBase
        PersistableBase.setSessionFactory(sessionFactory);
    }

    public void setEntityServices(Map<String, Object> entityServices) {
        this.entityServices = entityServices;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
}
