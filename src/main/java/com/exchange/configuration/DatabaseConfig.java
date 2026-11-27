package com.exchange.configuration;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.exchange.enums.ConstantValue;

@Configuration
public class DatabaseConfig {

	@Bean(name = "dbpostgres")
	public DataSource createUserServiceDataSource() {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(ConstantValue.DRIVER.getValue());
		dataSource.setUrl(ConstantValue.URL.getValue());
		dataSource.setUsername(ConstantValue.USERNAME.getValue());
		dataSource.setPassword(ConstantValue.PASSWORD.getValue());

		return dataSource;

	}

}
