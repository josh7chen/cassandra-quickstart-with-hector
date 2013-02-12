package com.lei.cassandra.sample;

import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;


/**
 * 
 * DemoCompositeInsertQuery is a example of composite insert and query   
 * 
 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoCompositeInsertQuery 
 * 
 * @author stones333
 *
 */
public class DemoCompositeInsertQuery extends DemoTest {

	/**
	 * build the query for inserting and fetching a row's columns  
	 * 
	 * @return QueryResult
	 */
    public QueryResult<ColumnSlice<Composite, String>> query() {
    	
    	CompositeSerializer cs = new CompositeSerializer();
    	Mutator<String> mutator = HFactory.createMutator(this.getKeyspace(), StringSerializer.get() );
        
    	HColumnImpl<Composite, String> column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
    	column.setClock(this.getKeyspace().createClock());
        Composite dc = new Composite();
        dc.add(0, "john");
        dc.add(1, 26L);
        dc.add(2, 68000L);
        column.setName(dc);       
        column.setValue("19175 Danube Dr, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
        
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(this.getKeyspace().createClock());
        dc = new Composite();
        dc.add(0, "victor");
        dc.add(1, 38L);
        dc.add(2, 109000L);
        column.setName(dc);       
        column.setValue("19217 Orange Ave, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
       
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(this.getKeyspace().createClock());
        dc = new Composite();
        dc.add(0, "mike");
        dc.add(1, 46L);
        dc.add(2, 439000L);
        column.setName(dc);       
        column.setValue("19990 Mira Vista Rd, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
       
        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(this.getKeyspace().createClock());
        dc = new Composite();
        dc.add(0, "joe");
        dc.add(1, 30L);
        dc.add(2, 215000L);
        column.setName(dc);       
        column.setValue("20123 Ann Arbor Ave, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);

        column = new HColumnImpl<Composite, String>(cs, StringSerializer.get());
        column.setClock(this.getKeyspace().createClock());
        dc = new Composite();
        dc.add(0, "jean");
        dc.add(1, 58L);
        dc.add(2, 150000L);
        column.setName(dc);
        column.setValue("26853 Woodlark Way, Cupertino, CA");
        mutator.addInsertion("CA:95014", "censor_info", column);
        
            
        mutator.execute();
        
        SliceQuery<String, Composite, String> sliceQuery = 
        		HFactory.createSliceQuery(this.getKeyspace(), StringSerializer.get(), cs, StringSerializer.get());
        sliceQuery.setColumnFamily("censor_info");
        sliceQuery.setKey("CA:95014");

        Composite startRange = new Composite();
        startRange.add(0, "a");

        Composite endRange = new Composite();
        endRange.add(0, "z");
        
        sliceQuery.setRange(startRange, endRange, false, 10);

        QueryResult<ColumnSlice<Composite, String>> result = sliceQuery.execute();
       
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
		
		DemoCompositeInsertQuery test = new DemoCompositeInsertQuery();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
