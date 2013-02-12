package com.lei.cassandra.sample;

import java.util.List;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * Demo MultigetSliceQuery with multiple keys:
 *
 * To run this example from maven:
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQueryMultiKeyColumns
 * 
 * @author stones333
 *
 */
public class DemoQueryMultiKeyColumns extends DemoTest {

	/**
	 * build the query with multiple keys
	 * 
	 * @return QueryResult
	 */
    public QueryResult<Rows<String,String,String>> query() {        
        MultigetSliceQuery<String, String, String> multigetSlicesQuery =
        		HFactory.createMultigetSliceQuery( this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        multigetSlicesQuery.setColumnFamily(DEF_USER_CF_NAME);
        multigetSlicesQuery.setColumnNames("email","full_name","gender","address", "state");        
        multigetSlicesQuery.setKeys("mary_leaman","john_smith");
        QueryResult<Rows<String, String, String>> results = multigetSlicesQuery.execute();
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
		
		DemoQueryMultiKeyColumns test = new DemoQueryMultiKeyColumns();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
