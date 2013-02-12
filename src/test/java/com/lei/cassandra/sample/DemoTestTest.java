package com.lei.cassandra.sample;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DemoTestTest 	{

	@BeforeClass
	static public void startUp() {
		System.out.println("startUp() ..... ");

	}
	
	@AfterClass
	static public void shutDown()  {
		System.out.println("tearDown() ..... ");
	}
	
	
	@Test
    public void testInsert()
    {
    	KeyspaceTest test = new KeyspaceTest();
    	
    	test.createKeyspace();
    	
    	System.out.println("Inserting ..... ");
		Map<String, String> customer = test.buildCustomerMap ("Tim", "Bird");
		test.printMapV2 (customer);
		test.update("tim_bird", customer);

		System.out.println("Retriving ...... ");
		Map<String, String> customer2 = test.readCustomer("tim_bird");
		test.printMapV2 (customer2);

		test.dropKeyspace();
        assertTrue( test.equalMaps(customer, customer2) );
		//assertTrue( true );
		
    }
    
	
    public void testResultStatus() {
    	DemoTest demoTest = new DemoTest(); 
    	demoTest.init();
    	List<Object> list = demoTest.doTest();
    	
    	assertTrue( list!=null && list.size()>0 );
    }

    @Test
    public void testDemoGetColumnValues() {
    	DemoGetColumnValues demoTest = new DemoGetColumnValues(); 
    	demoTest.init();
    	List<Object> list = demoTest.doTest();
    	
    	assertTrue( list!=null && list.size()>0 );
    }

    @Test
    public void testDemoInsertColumnValues() {
    	DemoInsertColumnValues test = new DemoInsertColumnValues();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    @Test
    public void testDemoDeleteColumnValues() {
    	DemoDeleteColumnValues test = new DemoDeleteColumnValues();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    @Test
    public void testDemoDeleteRow() {
    	DemoDeleteRow test = new DemoDeleteRow();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    @Test
    public void testDemoListAllColumnValues() {
    	DemoListAllColumnValues test = new DemoListAllColumnValues();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    
    @Test
    public void testDemoQueryRowColumnList() {
    	DemoQueryRowColumnList test = new DemoQueryRowColumnList();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    @Test
    public void testDemoQueryRowRangeColumnList() {
    	DemoQueryRowRangeColumnList test = new DemoQueryRowRangeColumnList();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    @Test
    public void testDemoRangeColumns() {
    	DemoRangeColumns test = new DemoRangeColumns();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    @Test
    public void testDemoQueryMultiKeyColumns() {
    	DemoQueryMultiKeyColumns test = new DemoQueryMultiKeyColumns();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    
    @Test
    public void testDemoQuerySecondaryIndex() {
    	DemoQuerySecondaryIndex test = new DemoQuerySecondaryIndex();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }

    
    
    @Test
    public void testDemoQuerySecondaryIndexRawBytes() {
    	DemoQuerySecondaryIndexRawBytes test = new DemoQuerySecondaryIndexRawBytes();
    	test.init();
    	List<Object> list = test.doTest();
    	assertTrue( list!=null && list.size()>0 );

    }
    
}
