package io.esgyn.coprocessor;

import io.ampool.monarch.table.MResultScanner;
import io.ampool.monarch.table.MScan;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.filter.KeyOnlyFilter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NumCallsCoprocessor extends MCoprocessor {

    static final Logger logger = LogManager.getLogger(NumCallsCoprocessor.class.getName());

    long m_calls = 0;

    public  NumCallsCoprocessor() {
    }

    public long numCalls(MCoprocessorContext context) {
	logger.info("NumCallsCoprocessor.numCalls ");

        MExecutionRequest request = context.getRequest();

	    logger.info("Table Name: " + 
			context.getTable().getName());
        try {
        } catch (Exception e) {
            throw new MCoprocessorException("Error in scanning results");
        }
        return ++m_calls;
    }

    @Override
	public String getId() {
	return this.getClass().getName();
    }

}
