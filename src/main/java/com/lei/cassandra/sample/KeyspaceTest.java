package com.lei.cassandra.sample;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * See http://www.datastax.com/docs/0.8/dml/using_cli
 * for reference   
 * 
 * @author stones333
 *
 */
// com.lei.cassandra.sample.KeyspaceTest
public class KeyspaceTest {

	public static final String DEF_LOCALHOST_ID = "localhost:9160";
	public static final String DEF_CLUSTER="Test Cluster";
	public static final String DEF_KEYSPACE_NAME="MyKeyspace";
	public static final String DEF_USER_CF_NAME = "Customer";
	
	public static final String DEF_COL_FIRST_NAME = "first_name";
	public static final String DEF_COL_LAST_NAME = "last_name";
	
	
	private Cluster clusterKeyspaceTest = null;
	private ColumnFamilyDefinition customerCFDef=null;
	private KeyspaceDefinition customerKSDef=null;
	private Keyspace customerKS=null;
	private ColumnFamilyTemplate<String, String> customerCFTemplate=null;
	
	public Map<String, String> buildCustomerMap (String firstName, String lastName) {
		Map<String, String> customer = new HashMap<String, String>();
		customer.put(DEF_COL_FIRST_NAME, firstName);
		customer.put(DEF_COL_LAST_NAME, lastName);
		return customer;
	}
	
	public void createKeyspace() {
		createKeyspace(DEF_LOCALHOST_ID, DEF_CLUSTER, DEF_KEYSPACE_NAME, DEF_USER_CF_NAME);
	}

	
	public void createKeyspace(String strHostID, String clusterName, String keyspaceName, String cfName) {
	    
		clusterKeyspaceTest = HFactory.getOrCreateCluster(clusterName, strHostID);
	    System.out.println("Cluster instantiated");
	    
	    customerCFDef = HFactory.createColumnFamilyDefinition(keyspaceName, cfName, ComparatorType.UTF8TYPE);
	    
	    customerKSDef = clusterKeyspaceTest.describeKeyspace(keyspaceName);
	    
	    if(customerKSDef == null){
	    	//if the keyspace doesn't exist, it creates one
	    	customerKSDef = HFactory.createKeyspaceDefinition(keyspaceName, 
	          ThriftKsDef.DEF_STRATEGY_CLASS, 1,
	          Arrays.asList(customerCFDef));
	    	clusterKeyspaceTest.addKeyspace(customerKSDef, true);
	    	System.out.println("Keyspace " +  keyspaceName + " created");
	    } else {
	    	
	    	try {
	    		clusterKeyspaceTest.addColumnFamily(customerCFDef);
	    	} catch (HectorException e) {
	    		System.out.println(e.getMessage());
	    	}
	    	System.out.println("cfName " +  cfName + " added");
	    }

	    customerKS = HFactory.createKeyspace(keyspaceName, clusterKeyspaceTest);
	    System.out.println("Keyspace " +  keyspaceName + " instantiated");

	    customerCFTemplate = new ThriftColumnFamilyTemplate<String, String>(customerKS,
	    		cfName, StringSerializer.get(), StringSerializer.get());
	}

	public String dropKeyspace () {
		System.out.println("Drop Keyspace " +  DEF_KEYSPACE_NAME );
		String retCode = clusterKeyspaceTest.dropKeyspace(DEF_KEYSPACE_NAME, true);
		System.out.println("Drop Keyspace return " +  retCode );
		return retCode;
	}
	
	
	public void update(String key, String firstName, String lastName) {
	    ColumnFamilyUpdater<String, String> updater = customerCFTemplate.createUpdater(key);
	    updater.setString(DEF_COL_FIRST_NAME, firstName);
	    updater.setString(DEF_COL_LAST_NAME, lastName);
	    try {
	    	customerCFTemplate.update(updater);
	    	System.out.println("value inserted");
	    } catch (HectorException e) {
	      System.out.println("Error during insertion");
	      System.out.println(e.getMessage());
	    }  
	}
	
	public void update(String key, Map<String, String> customer) {
		if (customer==null) { 
			return ;
		}
	    ColumnFamilyUpdater<String, String> updater = customerCFTemplate.createUpdater(key);
	    updater.setString(DEF_COL_FIRST_NAME, customer.get(DEF_COL_FIRST_NAME));
	    updater.setString(DEF_COL_LAST_NAME, customer.get(DEF_COL_LAST_NAME) );
	    try {
	    	customerCFTemplate.update(updater);
	    	System.out.println("value inserted");
	    } catch (HectorException e) {
	      System.out.println("Error during insertion");
	      System.out.println(e.getMessage());
	    }  
	}
	
	public void read(String key) {
		try {
			ColumnFamilyResult<String, String> result = customerCFTemplate.queryColumns(key);
			System.out.println("User: " + result.getString(DEF_COL_FIRST_NAME) + " " + result.getString(DEF_COL_LAST_NAME));
	    } catch (HectorException e) {
	    	System.out.println("Not possible to read the column with key " + key);
	    	System.out.println(e.getMessage());
	    }
	}

	
	public boolean equalMapsString(Map<String, String>m1, Map<String, String>m2) {
		   if (m1.size() != m2.size()) {
		      return false;
		   }
		   for (String key: m1.keySet()) {
		      if (!m1.get(key).equals(m2.get(key)))
		         return false;
		   }
		   return true;
	}
	
	public boolean equalMaps(Map<?, ?> map1, Map<?, ?>map2) {
	
		if (map1==null || map2==null || map1.size() != map2.size()) {
			return false;
		}
	
		for (Object key: map1.keySet()) {
			if (!map1.get(key).equals(map2.get(key))) {
				return false;
			}
		}
		return true;
	}
	
	
	public Map<String, String> readCustomer(String key) {
		Map<String, String> customer = new HashMap<String, String>();
		try {
			ColumnFamilyResult<String, String> result = customerCFTemplate.queryColumns(key);
			customer.put(DEF_COL_FIRST_NAME, result.getString(DEF_COL_FIRST_NAME));
			customer.put(DEF_COL_LAST_NAME, result.getString(DEF_COL_LAST_NAME));
	    } catch (HectorException e) {
	    	System.out.println("Not possible to read the column with key " + key);
	    	System.out.println(e.getMessage());
	    }
		return customer;
	}

	private void delete(String key) {
		try {
			customerCFTemplate.deleteRow(key);
		} catch (HectorException e) {
			System.out.println("Not possible to delete row with key " + key);
			System.out.println(e.getMessage());
		}
	}
	
//	public void printMap (Map <String, String> map) { 
//		for (Map.Entry<String,String> entry : map.entrySet()) {
//			String key = entry.getKey();
//			String value = entry.getValue();
//			System.out.println("key: " + key + " value: " + value);
//		}
//	}

	public void printMap (Map <?, ?> map) { 
		for (Map.Entry<?,?> entry : map.entrySet()) {
			System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
		}
	}

	public void printMapV2 (Map <?, ?> map) {
		StringBuilder sb = new StringBuilder(128);
		sb.append("{");
		for (Map.Entry<?,?> entry : map.entrySet()) {
			if (sb.length()>1) {
				sb.append(", ");
			}
			sb.append(entry.getKey()).append("=").append(entry.getValue());
		}
		sb.append("}");
		System.out.println(sb);
	}

	public static void main( String[] args )
	{
		KeyspaceTest test = new KeyspaceTest();
		System.out.println("instantiates cluster, keyspace and column family");
		test.createKeyspace();
		
		System.out.println("Insert a row in " + DEF_USER_CF_NAME);
	    test.update("key1", "alan", "chang");
	    
	    System.out.println("Reading data");
	    test.read("key1");
	    
	    System.out.println("Updating a row in" + DEF_USER_CF_NAME);
	    test.update("key1", "michelle", "gomez");
	    
	    System.out.println("Reading data");
	    test.read("key1");
	    
	    System.out.println("Deleteing a row in" + DEF_USER_CF_NAME);
	    test.delete("key1");
	    
	    
	    System.out.println("Reading non existing data");
	    test.read("key1");

	    
	    
	    System.out.println("");
	    String key = "tim_johnson";
		System.out.println("Retrive the customer " + key);
		Map<String, String> customer3 = test.readCustomer(key);
		test.printMap (customer3);

		System.out.println("");
		String first = "Tim";
		String last = "Johnson";
	    System.out.println("Insert new customer");
		Map<String, String> customer = test.buildCustomerMap (first, last);
		test.printMap (customer);
		test.update(key, customer);
		
		
		System.out.println("");
		System.out.println("Retrive the customer, again " + key);
		Map<String, String> customer2 = test.readCustomer(key);
		test.printMap (customer2);
		test.printMapV2 (customer2);


		System.out.println("");
		boolean ret = test.equalMaps(customer, customer2);
		System.out.println("equalMaps(customer, customer2): " + ret);
		
	    test.dropKeyspace ();
	}
	
}
