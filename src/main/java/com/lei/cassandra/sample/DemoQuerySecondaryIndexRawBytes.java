package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;


/**
 * 
 * # To query on a secondary index with raw bytes: 
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQuerySecondaryIndexRawBytes
 * 
 * @author stones333
 *
 */
public class DemoQuerySecondaryIndexRawBytes extends DemoTest {

	/**
	 * build the query for fetching row's columns on a secondary index with raw bytes 
	 * 
	 * @return QueryResult
	 */
    public QueryResult<OrderedRows<String, String, byte[]>> query() {
        IndexedSlicesQuery<String, String, byte[]> indexedSlicesQuery = 
            HFactory.createIndexedSlicesQuery(this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), BytesArraySerializer.get());
        indexedSlicesQuery.setColumnFamily("users");
        indexedSlicesQuery.setColumnNames("email","full_name","gender","address", "state");
        indexedSlicesQuery.addEqualsExpression("state", StringSerializer.get().toBytes("CA"));
        indexedSlicesQuery.setStartKey("");
        QueryResult<OrderedRows<String, String, byte[]>> result = indexedSlicesQuery.execute();
        
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
		
		DemoQuerySecondaryIndexRawBytes test = new DemoQuerySecondaryIndexRawBytes();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
