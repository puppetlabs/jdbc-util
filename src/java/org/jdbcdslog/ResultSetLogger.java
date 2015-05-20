package org.jdbcdslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultSetLogger {
    private static Logger logger = LoggerFactory.getLogger(ResultSetLogger.class);

    public static void debug(String s) {
        logger.debug(s + LogUtils.getStackTrace());
    }

    public static void info(String s) {
        logger.info(s + LogUtils.getStackTrace());
    }

    public static boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    public static boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public static void error(String m, Throwable t) {
        logger.error(m, t);
    }

    public static Logger getLogger() {
        return logger;
    }
}
