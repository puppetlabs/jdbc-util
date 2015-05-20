package org.jdbcdslog;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationParameters {

    /**
     * All JDBCDSLog Properties.
     *
     * @author ShunLi
     */
    protected interface JDBCDSLogProperties {
        String logText = "jdbcdslog.logText";
        String printStackTrace = "jdbcdslog.printStackTrace";
        String showTime = "jdbcdslog.showTime";
        String allowedMultiDbs = "jdbcdslog.allowedMultiDbs";
        String slowQueryThreshold = "jdbcdslog.slowQueryThreshold";
        String driverName = "jdbcdslog.driverName";
    }

    private static Logger logger = LoggerFactory.getLogger(ConfigurationParameters.class);
    private static Properties props;

    static long slowQueryThreshold = Long.MAX_VALUE;
    static boolean logText = false;
    static Boolean showTime = false;
    static boolean allowedMultiDbs = false;
    static boolean printStackTrace = false;
    static RdbmsSpecifics defaultRdbmsSpecifics = new OracleRdbmsSpecifics();
    static RdbmsSpecifics rdbmsSpecifics = defaultRdbmsSpecifics; // oracle is default db.

    static Map<String, RdbmsSpecifics> rdbmsSpecificsMap = new HashMap<String, RdbmsSpecifics>();

    static {
        ClassLoader loader = ConfigurationParameters.class.getClassLoader();
        InputStream in = null;
        try {
            in = loader.getResourceAsStream("jdbcdslog.properties");
            props = new Properties(System.getProperties());
            if (in != null) {
                props.load(in);
            }

            logText = getBooleanProp(JDBCDSLogProperties.logText, logText);
            printStackTrace = getBooleanProp(JDBCDSLogProperties.printStackTrace, printStackTrace);
            showTime = getBooleanProp(JDBCDSLogProperties.showTime, showTime);
            allowedMultiDbs = getBooleanProp(JDBCDSLogProperties.allowedMultiDbs, allowedMultiDbs);

            initSlowQueryThreshold();
            initRdbmsSpecificsMap();
            initRdbmsSpecifics();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
        }
    }

    /* init parameters start. */
    private static void initSlowQueryThreshold() {
        String isSlowQueryThreshold = props.getProperty(JDBCDSLogProperties.slowQueryThreshold);
        if (isSlowQueryThreshold != null && isLong(isSlowQueryThreshold)) {
            slowQueryThreshold = Long.parseLong(isSlowQueryThreshold);
        }
        if (slowQueryThreshold == -1) {
            slowQueryThreshold = Long.MAX_VALUE;
        }
    }

    private static boolean getBooleanProp(String propKey, boolean defaultValue) {
        return "true".equalsIgnoreCase(props.getProperty(propKey, String.valueOf(defaultValue)));
    }

    private static void initRdbmsSpecificsMap() {
        // key will turn Lower Case. and default rdbmsSpecifics is oralce , so exclude Oracle related.
        RdbmsSpecifics mySqlRdbmsSpecifics = new MySqlRdbmsSpecifics();
        RdbmsSpecifics sqlServerRdbmsSpecifics = new SqlServerRdbmsSpecifics();

        rdbmsSpecificsMap.put("oracle", defaultRdbmsSpecifics);
        rdbmsSpecificsMap.put("mysql", mySqlRdbmsSpecifics);
        rdbmsSpecificsMap.put("sqlserver", sqlServerRdbmsSpecifics);
        // if you have more rdbms specifice, defind it in here.
    }

    private static void initRdbmsSpecifics() {
        loadRdbmsSpecificsFromMap(props.getProperty(JDBCDSLogProperties.driverName));
    }

    /* init parameters end. */

    public static void setLogText(boolean alogText) {
        logText = alogText;
    }

    private static boolean isLong(String sSlowQueryThreshold) {
        try {
            Long.parseLong(sSlowQueryThreshold);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * support specific info should be got from connection specific database vendor
     *
     * @param driverName
     */
    public static void reloadRdbmsSpecificsFromConnection(String driverName) {
        if (logger.isDebugEnabled()) {
            logger.debug("Reload Rdbms Specifics From Connection: " + driverName);
        }
        loadRdbmsSpecificsFromMap(driverName);
    }

    /**
     * @param driverName
     */
    private static void loadRdbmsSpecificsFromMap(String driverName) {
        if (driverName != null) {
            String capitalizedDriverName = driverName.toLowerCase();

            if (rdbmsSpecificsMap.containsKey(capitalizedDriverName)) {
                rdbmsSpecifics = rdbmsSpecificsMap.get(capitalizedDriverName);
            } else {
                boolean find = false;

                for (String key : rdbmsSpecificsMap.keySet()) {
                    find = capitalizedDriverName.indexOf(key) >= 0;

                    if (find) {
                        rdbmsSpecifics = rdbmsSpecificsMap.get(key);
                        rdbmsSpecificsMap.put(capitalizedDriverName, rdbmsSpecifics);
                        break;
                    }
                }

                if (!find) {
                    rdbmsSpecificsMap.put(capitalizedDriverName, rdbmsSpecifics); // = the recent rdbms specifics.
                }
            }
        }
    }
}
