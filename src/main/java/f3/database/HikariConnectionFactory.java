/*
 * Copyright (c) 2010-2016 fork3
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR 
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package f3.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import f3.commons.database.AbstractConnectionFactory;
import f3.commons.database.ConnectionWrapper;
import lombok.extern.slf4j.Slf4j;

/**
 * @author n3k0nation
 *
 */
@Slf4j
public class HikariConnectionFactory extends AbstractConnectionFactory {
	private final ThreadLocal<ConnectionWrapper> localConnection = new ThreadLocal<>();
	private HikariDataSource source;
	
	@Override
	public void init() {
		try {
			if (maxConnections < 2) {
				maxConnections = 2;
			}

			HikariConfig config = new HikariConfig();

			config.setDataSourceClassName(databaseDriver);
			config.addDataSourceProperty("url", databaseUrl);
			config.addDataSourceProperty("user", databaseLogin);
			config.addDataSourceProperty("password", databasePassword);
			config.addDataSourceProperty("CacheCallableStatements", false);
			config.addDataSourceProperty("CachePreparedStatements", false);
			config.addDataSourceProperty("RetainStatementAfterResultSetClose", true);
			config.setMinimumIdle(Math.max(maxConnections / 3, 1));
			config.setMaximumPoolSize(maxConnections);
			config.setAutoCommit(true);
			config.setConnectionTimeout(5_000);
			config.setIdleTimeout(30_000);
			config.setMaxLifetime(TimeUnit.MINUTES.toMillis(5));
			config.setCatalog(catalog);

			source = new HikariDataSource(config);

			log.info ("ConnectionFactory: Connected to database server");
		} catch (Exception e) {
			log.error("ConnectionFactory: Failed to init database connections", e);
		}
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		ConnectionWrapper con = localConnection.get();
		if(con == null) {
			con = new ConnectionWrapper(e -> localConnection.remove(), source.getConnection());
		} else {
			con.use();
		}
		
		localConnection.set(con);
		return con;
	}
	
	@Override
	public DataSource getDataSource() {
		return source;
	}
	
	@Override
	public void shutdown() {
		source.close();
	}
}
