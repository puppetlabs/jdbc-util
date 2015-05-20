package org.jdbcdslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowQueryLogger {
    private static Logger logger = LoggerFactory.getLogger(SlowQueryLogger.class);

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
