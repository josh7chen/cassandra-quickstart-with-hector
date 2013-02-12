package com.lei.cassandra.sample;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * 
 * Demo list all column values 
 * 
 * # To list all column values:
 *  
 * $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoListAllColumnValues
 *
 * @author stones333
 *
 */ 
public class DemoListAllColumnValues extends DemoTest {

	private static final Map<String, AbstractSerializer<?>> ColumnToValueMapping = new HashMap<String, AbstractSerializer<?>>(){
		{
			put("birth_year", LongSerializer.get());
			put("email", StringSerializer.get() );
			put("full_name", StringSerializer.get() );
			put("gender", StringSerializer.get() );
			put("state", StringSerializer.get() );
		}
	};
	    
	
	static Object getColumnValue (String colName, ByteBuffer value) {
		AbstractSerializer<?> type = ColumnToValueMapping.get(colName);
		if (type==null) {
			type = StringSerializer.get();
		}
		return type.fromByteBuffer(value);
	}
	
    
	private List<Object> listAllEntries() {
		return listAllEntries(1000);
	}
	
	
	/**
	 * Demo to list all rows 
	 * 
	 * @param maxRowCount
	 * @return
	 */
	private List<Object> listAllEntries(int maxRowCount) {
    	
    	List<Object> list = new ArrayList<Object> ();

    	RangeSlicesQuery<String, String, ByteBuffer> rangeSlicesQuery = 
        		HFactory.createRangeSlicesQuery(this.getKeyspace(), StringSerializer.get(), StringSerializer.get(), ByteBufferSerializer.get())
            .setColumnFamily(DEF_USER_CF_NAME)
            .setRange(null, null, false, 10)
            .setRowCount(maxRowCount);

        String last_key = null;

        while (true) {
        	rangeSlicesQuery.setKeys(last_key, null);

        	QueryResult<OrderedRows<String, String, ByteBuffer>> result = rangeSlicesQuery.execute();
        	OrderedRows<String, String, ByteBuffer> rows = result.get();
        	Iterator<Row<String, String, ByteBuffer>> rowsIterator = rows.iterator();


            if (last_key != null && rowsIterator != null) {
            	rowsIterator.next();   
            }

            while (rowsIterator.hasNext()) {
            	Row<String, String, ByteBuffer> row = rowsIterator.next();
            	last_key = row.getKey();

            	if (row.getColumnSlice().getColumns().isEmpty()) {
            		continue;
            	}

            	System.out.println("rowKey [" + row.getKey() + "]");
            	
            	for(HColumn<String, ByteBuffer> cols : row.getColumnSlice().getColumns())
            	{
            		String colName = cols.getName();
        			System.out.println("colName [" + colName + "] colValue [" + getColumnValue(colName, cols.getValue() ) + "]");
            	}
            	list.add(row);
            	
            }

            if (rows.getCount() < maxRowCount) {
                break;
            }
        }
        
        return list;
    }

    
    public List<Object> doTest () {
    	return listAllEntries();
    }
    
	public static void main( String[] args ) {
		
		DemoListAllColumnValues test = new DemoListAllColumnValues();
		
		test.init();
		test.doTest();
		test.treminate();
	}

}
