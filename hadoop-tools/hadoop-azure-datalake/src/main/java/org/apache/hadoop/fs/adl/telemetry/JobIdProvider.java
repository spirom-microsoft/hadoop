package org.apache.hadoop.fs.adl.telemetry;

import org.apache.hadoop.classification.InterfaceAudience;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@InterfaceAudience.Private
public class JobIdProvider {

    /**
     * In general, a factory for getting the hopefully unique ID of the current job.
     * Implementations may return null always or sometimes.
     */
    private interface IdFactory {
        /**
         * Get the job ID
         * @return Job ID or null
         */
        String getJobId();
    }

    /**
     * Where we can't figure out what kind of job context we're running in, or we know
     * it's one where we don't have a way to get the Job ID. Always returns null.
     */
    private static class NullIdFactory implements IdFactory {
        /**
         * Always returns null for the Job ID as there's no way to figure it out
         * @return null
         */
        @Override
        public String getJobId() { return null; }
    }

    /**
     * If we're running in a Spark job (defined as having SparkContext in the classpath),
     * this will attempt to get a Job ID. It is still not guaranteed to succeed on every call,
     * as it may be an old version of Spark (pre 1.4) or the application may not actually be a
     * Spark job despite having core Spark in the classpath. In those cases it returns null.
     *
     * Callers should expect to create a different implementation if the constructor throws,
     * possibly just NullIdFactory as a fallback.
     */
    private static class SparkIdFactory implements IdFactory {
        SparkIdFactory() throws ClassNotFoundException, NoSuchMethodException {
            Class cSparkContext = Class.forName("org.apache.spark.SparkContext");
            _mGetOrCreate = cSparkContext.getMethod("getOrCreate");
            _mApplicationId = cSparkContext.getMethod("applicationId");
        }

        /**
         * Usually creates a Job ID based on SparkContext.getOrCreate().applicationId()
         * but may return null if it can't get one. Note that Spark clusters will create the
         * Application ID automatically, but it CAN be set by the user, so uniqueness is not
         * actually guaranteed.
         * @return Job ID or null
         */
        @Override
        public String getJobId() {
            // TODO: what to do if either of these throws? Maybe just return null?
            try {
                _oSparkContext = _mGetOrCreate.invoke(null);
                String appId = (String) _mApplicationId.invoke(_oSparkContext);
                return appId;
            } catch (InvocationTargetException ite) {
                // TODO: log it
            } catch (IllegalAccessException iae) {
                // TODO: log it
            }
            return null; // should have worked, but failed to get it
        }

        private Object _oSparkContext;
        private Method _mGetOrCreate;
        private Method _mApplicationId;
    }

    /**
     * Provides an ID for the job on whose behalf it is called, if possible.
     * @return Job ID or null
     */
    public static String getJobId() {
        // rather than locking we tolerate the timing artifacts; there seems little risk of a
        // more powerful factory being replaced by a less powerful one since the factory choice
        // should be made purely on what code can be loaded
        if (_idFactory == null) {
            try {
                IdFactory candidate = new SparkIdFactory();
                // didn't throw so promote it
                _idFactory = candidate;
            } catch (ClassNotFoundException cnfe) {
                // TODO: log it
            } catch (NoSuchMethodException nsme) {
                // TODO: log it
            }
        }
        if (_idFactory == null) {
            _idFactory = new NullIdFactory();
        }
        return _idFactory.getJobId();
    }

    private static IdFactory _idFactory = null;
}
