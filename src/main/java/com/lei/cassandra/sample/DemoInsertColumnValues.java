package com.lei.cassandra.sample;


import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Demo to insert a row and it's columns 
 * 
 * # To insert a row and it's columns:
 *  
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoInsertColumnValues
 *
 *
 * @author stones333
 *
 */
public class DemoInsertColumnValues extends DemoGetColumnValues {

	/**
	 * insert a row and its columns for the rowkey 
	 * 
	 * @return row's column values associated the rowkey
	 */
    public List<Object> insertRow () {
    	String rowkey = "mike_johnson";
    	
    	ColumnFamilyTemplate<String, String> userCFTemplate = new ThriftColumnFamilyTemplate<String, String>(
    			this.getKeyspace(),
    			DEF_USER_CF_NAME, StringSerializer.get(), StringSerializer.get());

    	ColumnFamilyUpdater<String, String> updater = userCFTemplate.createUpdater(rowkey);
	    updater.setString("full_name", "mike johnson");
	    updater.setString("address", "90 Elm Street");
	    updater.setString("state", "NY");
	    updater.setString("gender", "male");
	    updater.setString("email", "mikej@gmail.com");
	    updater.setLong("birth_year", new Long(1980));
	    try {
	    	userCFTemplate.update(updater);
	    	System.out.println("value inserted");
	    } catch (HectorException e) {
	    	System.out.println("Error during insertion : " + e.getMessage() );

	    }  
	    
    	return this.getColumnValues("mike_johnson");
    }


	/**
	 * insert a row and its columns with the rowkey using Mutator  
	 * 
	 * @return row's column values associated the rowkey
	 */
    public List<Object> insertRowV2 () {
    	Mutator<String> mutator = HFactory.createMutator(this.getKeyspace(), StringSerializer.get() );
    	mutator.addInsertion("michael_johnson", "users", HFactory.createStringColumn("full_name", "michael johnson"));
    	mutator.addInsertion("michael_johnson", "users", HFactory.createStringColumn("address", "9890 Elm Street"));
    	mutator.addInsertion("michael_johnson", "users", HFactory.createStringColumn("state", "CT"));
    	mutator.addInsertion("michael_johnson", "users", HFactory.createStringColumn("gender", "male"));
    	mutator.addInsertion("michael_johnson", "users", HFactory.createStringColumn("email", "michael_johnson@gmail.com"));
    	mutator.addInsertion("michael_johnson", "users", HFactory.createColumn("birth_year", new Long(1978), StringSerializer.get(), LongSerializer.get()));
    	
    	MutationResult mr = mutator.execute();
    	
    	return this.getColumnValues("michael_johnson");
    }

    public List<Object> doTest () {
    	//return insertRow ();
    	return insertRowV2 ();
    }

	public static void main( String[] args ) {
		
		DemoInsertColumnValues test = new DemoInsertColumnValues();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
