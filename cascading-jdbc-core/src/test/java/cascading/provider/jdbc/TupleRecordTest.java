package cascading.provider.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Test;

import cascading.tuple.Tuple;


public class TupleRecordTest {

	@Test
	public void testTupleRecord()
	  {
		
		Tuple tup = new Tuple();
		TupleRecord tupleRecord = new TupleRecord();
		
		tupleRecord.setTuple(tup);;
		assertSame(tup, tupleRecord.getTuple());
		
	  }
		
   @Test
   public void testWrite() throws SQLException
     {
  	    Tuple t = new Tuple("one", "two", "three");
  	 		PreparedStatement stmt = mock(PreparedStatement.class);
  	 		TupleRecord tupleRecord = new TupleRecord(t);
  	 		tupleRecord.write(stmt);
  	 		verify(stmt).setObject(1, "one");
  	 		verify(stmt).setObject(2, "two");
  	 		verify(stmt).setObject(3, "three");
  	 		verifyNoMoreInteractions(stmt);
     }
	
   @Test
   public void testRead() throws SQLException
     {
  	 Tuple expectedTuple = new Tuple("foo", "bar", "baz");
  	 
  	 ResultSet resultSet = mock(ResultSet.class);
  	 ResultSetMetaData rsm = mock(ResultSetMetaData.class);
  	 when(rsm.getColumnCount()).thenReturn(3);
  	 when(resultSet.getMetaData()).thenReturn(rsm);
  	 when(resultSet.getObject(1)).thenReturn("foo");
  	 when(resultSet.getObject(2)).thenReturn("bar");
  	 when(resultSet.getObject(3)).thenReturn("baz");
  	 
  	 TupleRecord tupleRecord = new TupleRecord();
  	 
  	 tupleRecord.readFields(resultSet);
  	 
  	 Tuple result = tupleRecord.getTuple();
  	 
  	 assertEquals(expectedTuple, result);
  	 
     }
	
}
