/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cascading.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Test;

import cascading.jdbc.TupleRecord;
import cascading.tuple.Tuple;

public class TupleRecordTest
  {

    @Test
    public void testTupleRecord()
      {

        Tuple tup = new Tuple();
        TupleRecord tupleRecord = new TupleRecord();

        tupleRecord.setTuple(tup);
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
