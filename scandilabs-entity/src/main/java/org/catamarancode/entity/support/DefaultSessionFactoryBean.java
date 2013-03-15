package org.catamarancode.entity.support;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;

/**
 * A session factory bean that sets various properties to sensible defaults,
 * thus helping to make spring configurations shorter
 * 
 * @author mkvalsvik
 * 
 */
public class DefaultSessionFactoryBean extends LocalSessionFactoryBean {

    private static Logger logger = LoggerFactory
            .getLogger(DefaultSessionFactoryBean.class);

    public DefaultSessionFactoryBean() {

        // Populate hibernate properties with sensible defaults
        Properties hibernateProperties = new Properties();
        hibernateProperties.setProperty("hibernate.dialect",
                "org.hibernate.dialect.MySQLDialect");
        hibernateProperties.setProperty("hibernate.connection.release_mode",
                "after_transaction");
        hibernateProperties.setProperty(
                "hibernate.bytecode.use_reflection_optimizer", "false");
        hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "update");
        hibernateProperties.setProperty("hibernate.show_sql", "false");
        hibernateProperties.setProperty("hibernate.format_sql", "false");
        hibernateProperties.setProperty("hibernate.jbdc.batch_size", "50");
        hibernateProperties.setProperty("hibernate.connection.autocommit",
                "false");
        /* http://stackoverflow.com/questions/10315696/spring-3-1-1-with-hibernate-4-1-annotations-configuration
        hibernateProperties.setProperty(
                "hibernate.current_session_context_class", "thread");
        */
        
        // see http://koenserneels.blogspot.com/2012/05/migrating-from-hibernate-3-to-4-with.html
        //hibernateProperties.setProperty("hibernate.transaction.jta.platform", "org.hibernate.service.jta.platform.internal.SunOneJtaPlatform");
        this.setHibernateProperties(hibernateProperties);

    }

}
