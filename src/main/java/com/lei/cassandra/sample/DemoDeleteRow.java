package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Demo to delete a row 
 * 
 * # To delete a row column:
 *  
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoDeleteRow
 * 
 * @author stones333
 *
 */
public class DemoDeleteRow extends DemoInsertColumnValues {

	/**
	 * delete the row with rowkey
	 * 
	 * @return row's column values associated the rowkey
	 */
    public List<Object> deleteRow() {
    	
    	List<Object> list = this.insertRow();
    	String rowkey = "mike_johnson";
    	ColumnFamilyTemplate<String, String> userCFTemplate = new ThriftColumnFamilyTemplate<String, String>(
    			this.getKeyspace(),
    			DEF_USER_CF_NAME, StringSerializer.get(), StringSerializer.get());
	    try {
	    	userCFTemplate.deleteRow(rowkey) ;
	    	System.out.println("Row deleted .... ");
	    } catch (HectorException e) {
	    	System.out.println("Error during deletion : " + e.getMessage() );
	    }  
	    
    	return this.getColumnValues("mike_johnson");
    }

	/**
	 * delete the row with rowkey using Mutator
	 * 
	 * @return row's column values associated the rowkey
	 */
    public List<Object> deleteRowV2() {
    	
    	List<Object> list = this.insertRowV2();
    	String rowkey = "michael_johnson";
	    
        Mutator<String> mutator = HFactory.createMutator(this.getKeyspace(), StringSerializer.get() );
        mutator.addDeletion(rowkey, "users", null, StringSerializer.get() );
        MutationResult mr = mutator.execute();
    	return this.getColumnValues(rowkey);
    }

    
    public List<Object> doTest () {
    	//return deleteRow ();
    	return deleteRowV2 ();
    }

	public static void main( String[] args ) {
		
		DemoDeleteRow test = new DemoDeleteRow();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
