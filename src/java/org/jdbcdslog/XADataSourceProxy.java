package org.jdbcdslog;

import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.sql.XADataSource;

public class XADataSourceProxy extends DataSourceProxyBase implements XADataSource, DataSource {

    private static final long serialVersionUID = -2923593005281631348L;

    public XADataSourceProxy() throws JDBCDSLogException {
        super();
    }

    public Logger getParentLogger() {
        return null;
    }

    public Object unwrap(Class targetClass) {
        return null;
    }

}
