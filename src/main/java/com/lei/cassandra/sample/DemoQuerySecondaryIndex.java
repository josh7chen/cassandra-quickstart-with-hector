package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * # To query on a secondary index: 
 * 
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQuerySecondaryIndex
 * 
 * @author stones333
 *
 */
public class DemoQuerySecondaryIndex extends DemoTest {

	/**
	 * build the query for fetching row's columns on a secondary index
	 * 
	 * @return QueryResult
	 */
    public QueryResult<OrderedRows<String, String, String>> query() {
        IndexedSlicesQuery<String, String, String> indexedSlicesQuery = 
            HFactory.createIndexedSlicesQuery(this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        indexedSlicesQuery.setColumnFamily("users");
        indexedSlicesQuery.setColumnNames("email","full_name","gender","address", "state");
        indexedSlicesQuery.addEqualsExpression("state", "CA");
        indexedSlicesQuery.setStartKey("");
        QueryResult<OrderedRows<String, String, String>> result = indexedSlicesQuery.execute();
        
        return result;
    }
    
    
    public List<Object> doTest() {
    	ResultStatus result = query();
        if ( result != null) {
            return printToLog(result);
        }
        return null;
    }

    
	public static void main( String[] args ) {
		
		DemoQuerySecondaryIndex test = new DemoQuerySecondaryIndex();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
