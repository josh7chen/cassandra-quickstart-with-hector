package com.lei.cassandra.sample;

import java.util.LinkedList;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.exceptions.HectorException;

/**
 * Demo to fetch column values for a rowkey 
 * 
 * # To fetch column values for a rowkey:
 *  
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoGetColumnValues
 *
 * @author stones333
 *
 */ 
public class DemoGetColumnValues extends DemoTest {

	/**
	 * getColumnVlues as a list objects from the rowkey
	 *  
	 * @param rowkey
	 * 
	 * @return row's column values associated the rowkey
	 */
    public List<Object> getColumnValues (String rowkey) {
    	List<Object> listColumnObj = new LinkedList<Object> ();
    	
		try {
	    	ColumnFamilyTemplate<String, String> userCFTemplate = new ThriftColumnFamilyTemplate<String, String>(
	    			this.getKeyspace(),
	    			DEF_USER_CF_NAME, StringSerializer.get(), StringSerializer.get());

			ColumnFamilyResult<String, String> result = userCFTemplate.queryColumns(rowkey);
			String full_name = result.getString("full_name");
			String address = result.getString("address");
			String state = result.getString("state");
			String gender = result.getString("gender");
			String email = result.getString("email");
			Long birth_year = result.getLong("birth_year");
			
			System.out.println("full_name [" + full_name + "]");
			System.out.println("address [" + address + "]");
			System.out.println("state [" + state + "]");
			System.out.println("gender [" + gender + "]");
			System.out.println("birth_year [" + birth_year + "]");
			System.out.println("email [" + email + "]");
			
			listColumnObj.add(full_name);
			listColumnObj.add(address);
			listColumnObj.add(state);
			listColumnObj.add(gender);
			listColumnObj.add(email);
			listColumnObj.add(birth_year);
			
	    } catch (HectorException e) {
	    	System.out.println("Error reading the column with key " + rowkey);
	    	System.out.println(e.getMessage());
	    }

		return listColumnObj;
    }
	
    public List<Object> doTest () {
    	return getColumnValues ("john_smith");
    }

	public static void main( String[] args ) {
		
		DemoGetColumnValues test = new DemoGetColumnValues();
		
		test.init();
		test.doTest();
		test.treminate();
	}
}
