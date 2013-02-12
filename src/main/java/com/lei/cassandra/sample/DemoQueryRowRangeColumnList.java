package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * 
 * # To query for fetching a rows' columns
 * 
 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQueryRowRangeColumnList
 * 
 * @author stones333
 *
 */
public class DemoQueryRowRangeColumnList extends DemoTest {
	
	/**
	 * build the query for fetching a rows' columns 
	 * 
	 * @return QueryResult
	 */
    public QueryResult<OrderedRows<String,String,String>> query() {
    	
        RangeSlicesQuery<String, String, String> rangeSlicesQuery =
                HFactory.createRangeSlicesQuery(this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());

        rangeSlicesQuery.setColumnFamily(DEF_USER_CF_NAME);
        rangeSlicesQuery.setColumnNames("email","full_name","gender","address", "state");        
        rangeSlicesQuery.setKeys("mary_leaman", "john_smith");
        rangeSlicesQuery.setRowCount(5);
        QueryResult<OrderedRows<String, String, String>> results = rangeSlicesQuery.execute();

        return results;
    }

    public List<Object> doTest() {
    	ResultStatus result = query();
        if ( result != null) {
            return printToLog(result);
        }
        return null;
    }

    
	public static void main( String[] args ) {
		
		DemoTest test = new DemoQueryRowRangeColumnList();
		
		test.init();
		test.doTest();
		test.treminate();
	}


}
