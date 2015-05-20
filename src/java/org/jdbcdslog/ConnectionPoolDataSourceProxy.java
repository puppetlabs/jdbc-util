package org.jdbcdslog;

import java.util.logging.Logger;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

public class ConnectionPoolDataSourceProxy extends DataSourceProxyBase implements DataSource, ConnectionPoolDataSource {

    private static final long serialVersionUID = 5094791657099299920L;

    public ConnectionPoolDataSourceProxy() throws JDBCDSLogException {
        super();
    }

    public Logger getParentLogger() {
        return null;
    }

    public Object unwrap(Class targetClass) {
        return null;
    }
}
