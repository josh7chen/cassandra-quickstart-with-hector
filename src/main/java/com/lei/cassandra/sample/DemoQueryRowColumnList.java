package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * 
 * # To query a column value for a key:
 *
 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQueryRowColumnList
 *
 * 
 * @author stones333
 *
 */
public class DemoQueryRowColumnList extends DemoTest {
	
	/**
	 * build the query for fetching a row's columns 
	 * 
	 * @return QueryResult
	 */
    public QueryResult<ColumnSlice<String,String>> query() {
        SliceQuery<String, String, String> sliceQuery = 
            HFactory.createSliceQuery(this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(DEF_USER_CF_NAME);
        sliceQuery.setKey("mary_leaman");
        sliceQuery.setColumnNames("email","full_name","gender","address", "state");
        QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
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
		
		DemoQueryRowColumnList test = new DemoQueryRowColumnList();
		
		test.init();
		test.doTest();
		test.treminate();
	}


}
