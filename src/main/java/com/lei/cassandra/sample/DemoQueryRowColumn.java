package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * # To query a column value for a key:
 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQueryRowColumn
 * 
 * @author stones333
 *
 */
public class DemoQueryRowColumn extends DemoTest {

	/**
	 * build the query for fetching a row's column
	 * 
	 * @return QueryResult
	 */
    public QueryResult<HColumn<String,String>> query() {        
        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(this.getKeyspace());
        columnQuery.setColumnFamily(DEF_USER_CF_NAME);
        columnQuery.setKey("john_smith");
        columnQuery.setName("full_name");
        QueryResult<HColumn<String, String>> result = columnQuery.execute();
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
		
		DemoQueryRowColumn test = new DemoQueryRowColumn();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
