package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.exceptions.HectorException;


/**
 * 
 * # Demo to delete row's columns:
 *  
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoDeleteColumnValues
 * 
 * @author stones333
 *
 */
public class DemoDeleteColumnValues extends DemoInsertColumnValues {

	/**
	 * delete a column value associated with rowkey 
	 * 
	 * @return row's column values associated the rowkey 
	 */
    public List<Object> deleteRowColumn () {
    	
    	List<Object> list = this.insertRow();
    	String rowkey = "mike_johnson";
    	
    	ColumnFamilyTemplate<String, String> userCFTemplate = new ThriftColumnFamilyTemplate<String, String>(
    			this.getKeyspace(),
    			DEF_USER_CF_NAME, StringSerializer.get(), StringSerializer.get());

	    try {
	    	userCFTemplate.deleteColumn(rowkey, "email") ;
	    	System.out.println("Column deleted .... ");
	    } catch (HectorException e) {
	    	System.out.println("Error during deletion : " + e.getMessage() );

	    }  
    	return this.getColumnValues("mike_johnson");
    }

    public List<Object> doTest () {
    	return deleteRowColumn ();
    }

	public static void main( String[] args ) {
		
		DemoDeleteColumnValues test = new DemoDeleteColumnValues();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
