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
 * DemoRangeColumns is to show how query use the comparator for ranges of text queries
 * 
 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoRangeColumns 
 * 
 * @author stones333
 *
 */
public class DemoRangeColumns extends DemoTest {

	/**
	 * build the query for fetching a row's columns in the range 
	 * 
	 * @return QueryResult
	 */
    public QueryResult<ColumnSlice<String,String>> query() {
        SliceQuery<String, String, String> sliceQuery = 
                HFactory.createSliceQuery(this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
            sliceQuery.setColumnFamily("stock_close_prices");
            sliceQuery.setKey("GOOG");
            // change the order argument to 'true' to get the last 2 columns in descending order
            // gets the first 4 columns "between" 2013-02- and 2013-02-28 according to comparator
            sliceQuery.setRange("2013-02-", "2013-02-28", false, 5);

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
		
		DemoRangeColumns test = new DemoRangeColumns();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
