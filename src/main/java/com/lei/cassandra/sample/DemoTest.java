package com.lei.cassandra.sample;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;







import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.HColumnFamilyImpl;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HColumnFamily;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;


/**
 * 
 * 
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoTest
 * 
 * @author stones333
 *
 */
public class DemoTest {
	
    private static Logger log = LoggerFactory.getLogger(DemoTest.class);
    
	public static final String DEF_LOCALHOST_ID = "localhost:9160";
	public static final String DEF_CLUSTER="Test Cluster";
	public static final String DEF_KEYSPACE_NAME="demo";
	public static final String DEF_USER_CF_NAME = "users";
	
	private Cluster cluster = null;
	private Keyspace keyspace;

	

	public Cluster getCluster() {
		return cluster;
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}
	
	/**
	 * Connect to Cassandra's demo keyspace at localhost's cluster at 9160  
	 */
	public void connect() {
		cluster = HFactory.getOrCreateCluster(DEF_CLUSTER, DEF_LOCALHOST_ID);
		System.out.println("Cluster instantiated");
		ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
		ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		keyspace = HFactory.createKeyspace("demo", cluster, ccl);
	}

	/**
	 * Disconnect from localhost's Cassandra cluster at 9160  
	 */
	public void disconnect() {
		cluster.getConnectionManager().shutdown();
	}
	
	public void init() {
		connect();
	}

	public void treminate() {
		disconnect();
	}

    
    public List<Object> doTest() {
    	ResultStatus result = execute();
        if ( result != null) {
            return printToLog(result);
        }
        return null;
    }

    
    public ResultStatus doQuery() {
        HColumnFamily<String, String> columnFamily = 
            new HColumnFamilyImpl<String, String>(keyspace, "users", StringSerializer.get(), StringSerializer.get() );
        columnFamily.addKey("john_smith");
        
        columnFamily.addColumnName("email")
        .addColumnName("state")
        .addColumnName("gender")
        .addColumnName("address");
        
        log.info("Results from HColumnFamily: email: {} state:{} gender:{} address:{}",
                new Object[]{columnFamily.getString("email"),
                columnFamily.getString("state"),
                columnFamily.getString("gender"),
                columnFamily.getString("address")});
        return columnFamily;
    }


    
    public QueryResult<ColumnSlice<Composite, String>> execute() {
    	
    	CompositeSerializer cs = new CompositeSerializer();
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get() );
        
        HColumnImpl<Composite, String> column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(keyspace.createClock());
        Composite dc = new Composite();
        dc.add(0, "john");
        dc.add(1, 26L);
        dc.add(2, 68000L);
        column.setName(dc);       
        column.setValue("19175 Danube Dr, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
        
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(keyspace.createClock());
        dc = new Composite();
        dc.add(0, "victor");
        dc.add(1, 38L);
        dc.add(2, 109000L);
        column.setName(dc);       
        column.setValue("19217 Orange Ave, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
       
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(keyspace.createClock());
        dc = new Composite();
        dc.add(0, "mike");
        dc.add(1, 46L);
        dc.add(2, 439000L);
        column.setName(dc);       
        column.setValue("19990 Mira Vista Rd, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
       
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(keyspace.createClock());
        dc = new Composite();
        dc.add(0, "joe");
        dc.add(1, 30L);
        dc.add(2, 215000L);
        column.setName(dc);       
        column.setValue("20123 Ann Arbor Ave, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);

        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(keyspace.createClock());
        dc = new Composite();
        dc.add(0, "jean");
        dc.add(1, 58L);
        dc.add(2, 150000L);
        column.setName(dc);
        column.setValue("26853 Woodlark Way, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
        
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(keyspace.createClock());
        dc = new Composite();
        dc.add(0, "shiba");
        dc.add(1, 33L);
        dc.add(2, 227000L);
        column.setName(dc);       
        column.setValue("10300 Byrne Ave, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
            
        mutator.execute();
        
        SliceQuery<String, Composite, String> sliceQuery = 
          HFactory.createSliceQuery(keyspace, StringSerializer.get(), cs, StringSerializer.get());
        sliceQuery.setColumnFamily("censor_info");
        sliceQuery.setKey("CA:95014");

        Composite startRange = new Composite();
        startRange.add(0, "a");
        startRange.addComponent(new Long(50), LongSerializer.get(), "LongType", AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);

        Composite endRange = new Composite();
        endRange.add(0, "z");
        
        sliceQuery.setRange(startRange, endRange, false, 10);

        QueryResult<ColumnSlice<Composite, String>> result = sliceQuery.execute();
       
        return result;

	}

    

        
    @SuppressWarnings("unchecked")
	public List<Object> printToLog(ResultStatus result) {
    	
        log.info("=======================================================================");
    	List<Object> listObj = new LinkedList<Object> ();
    	
    	log.info("Result executed in: {} microseconds against host: {}",                
    			result.getExecutionTimeMicro(), result.getHostUsed().getName());
        

        // display of Rows vs. HColumn or ColumnSlice
        if ( result instanceof QueryResult ) {
        	log.info("QueryResult : {} ", ((QueryResult) result).get());
            QueryResult<?> qr = (QueryResult) result;
            if ( qr.get() instanceof Rows ) {
                Rows<?,?,?> rows = (Rows)qr.get();
                for (Row row : rows) {
                  log.info("Row key: {}", row.getKey());
                  for ( Iterator iter = row.getColumnSlice().getColumns().iterator(); iter.hasNext();) {
                	  HColumn<?,?> obj = (HColumn<?,?>) iter.next();
                	  //Object obj = iter.next();
                	  log.info("Column - {}", obj);
                	  
                	  if (obj.getValue() instanceof byte[]) {
                		  log.info("Column - name [{}], decoded value [{}]", obj.getName(), StringSerializer.get().fromBytes( (byte[]) obj.getValue()   ) );
                	  } else {
                		  log.info("Column - name [{}], value [{}]", obj.getName(), obj.getValue() );
                	  }
                	  listObj.add(obj);
                  }
                }
            } else if ( qr.get() instanceof ColumnSlice ) {
              
            	for ( Iterator iter = ((ColumnSlice)qr.get()).getColumns().iterator(); iter.hasNext();) {
            		HColumn<?,?> obj = (HColumn<?,?>) iter.next();
            		log.info("Column: {}", obj);
            		log.info("Column: name [{}], value [{}]", obj.getName(), obj.getValue() );
            		listObj.add(obj);
              }
            } else if ( qr.get() instanceof HColumn ) {
            	
            	//Object obj = qr.get();
            	HColumn c = (HColumn) qr.get();
            	
                log.info("Result: {}", c );
                log.info("Name [{}], Value [{}]", c.getName(), c.getValue() );
                listObj.add(c);
            }
        }
        log.info("=======================================================================");
        return listObj;
    }

    

    
    public static void main( String[] args ) {

    	DemoTest test = new DemoTest();
    	test.init();
    	
    	List<Object> list = test.doTest();
    	//System.out.println("list.size() " + list.size());
    	test.doQuery();
        test.treminate();
        
    }

}
