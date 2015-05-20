package org.jdbcdslog;

import java.util.logging.Logger;
import javax.sql.DataSource;

public class DataSourceProxy extends DataSourceProxyBase implements DataSource {

    private static final long serialVersionUID = -6888072076120346186L;

    public DataSourceProxy() throws JDBCDSLogException {
        super();
    }

    public Logger getParentLogger() {
        return null;
    }

    public Object unwrap(Class targetClass) {
        return null;
    }

}
