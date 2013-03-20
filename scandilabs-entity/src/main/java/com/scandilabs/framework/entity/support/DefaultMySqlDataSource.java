package com.scandilabs.framework.entity.support;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * A datasource that defaults to the MySQL driver and default user, thus helping
 * to make spring configurations shorter
 * 
 * @author mkvalsvik
 * 
 */
public class DefaultMySqlDataSource extends DriverManagerDataSource {

    public DefaultMySqlDataSource() {
        this.setDriverClassName("com.mysql.jdbc.Driver");
        this.setUsername("root");
    }

}
