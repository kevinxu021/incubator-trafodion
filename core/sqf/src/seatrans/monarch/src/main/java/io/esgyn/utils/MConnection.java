package io.esgyn.utils;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MConnection {

    static final Log LOG = LogFactory.getLog(MConnection.class);

    static MConfiguration sv_mconf = null;
    static MClientCache sv_clientCache = null;

    public static MConfiguration getMConfiguration() {
	if (sv_mconf == null) {
	    sv_mconf = MConfiguration.create();
	}

	return sv_mconf;
    }

    public static MClientCache createClientCache(BagOfArgs p_pa) {

        if (LOG.isTraceEnabled()) LOG.trace("ENTER: MConnection.createClientCache");

	if (sv_clientCache != null) {
	    return sv_clientCache;
	}

        MConfiguration mconf = MConnection.getMConfiguration();
	mconf.addResource("ampool-site.xml");
	if (LOG.isInfoEnabled()) LOG.info("createClientCache"
					   + ", locator port from config: " + mconf.getInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT)
					   );
	
        //mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, p_pa.m_locator_host);
        //mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, p_pa.m_locator_port);
	if ((p_pa.m_log_file_name != null) &&
	    (p_pa.m_log_file_name.length() > 0)) {
	    mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, p_pa.m_log_file_name);
	}

	try {

        sv_clientCache = MClientCacheFactory.getOrCreate(mconf);

	}
	catch (com.gemstone.gemfire.cache.CacheClosedException m_cce) {
	    LOG.error("Error while creating Monarch cache", m_cce);
	}
	
        if (LOG.isTraceEnabled()) LOG.trace("EXIT: MConnection.createClientCache"
					    + ", clientCache: " + (sv_clientCache != null ? sv_clientCache:"null"));
	return sv_clientCache;
    }
}
