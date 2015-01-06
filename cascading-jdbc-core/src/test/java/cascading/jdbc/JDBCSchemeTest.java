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

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.DBOutputFormat;
import cascading.tuple.Fields;

public class JDBCSchemeTest
  {

  @SuppressWarnings("unchecked")
  @Test
  public void testPresentSinkFields()
    {
    String[] columnNames = new String[]{ "id", "firstname", "lastname" };
    JDBCScheme scheme = new JDBCScheme( DBInputFormat.class, DBOutputFormat.class, Fields.UNKNOWN, columnNames, null, null, -1, null, null,
        null );

    @SuppressWarnings("rawtypes")
    Class[] fieldTypes = new Class<?>[]{ int.class, String.class, String.class };
    Fields fields = new Fields( columnNames, fieldTypes );
    FlowProcess<JobConf> fp = mock( FlowProcess.class );

    JDBCTap tap = mock( JDBCTap.class );

    TableDesc desc = new TableDesc( "test_table" );
    when( tap.getTableDesc() ).thenReturn( desc );
    
    assertFalse( desc.hasRequiredTableInformation() );

    scheme.presentSinkFields( fp, tap, fields );

    assertTrue( desc.hasRequiredTableInformation() );
    assertEquals( fields, scheme.getSinkFields() );

    assertArrayEquals( columnNames, desc.getColumnNames() );

    assertArrayEquals( new String[]{ "int not null", "varchar(256)", "varchar(256)" }, desc.getColumnDefs() );
    
    }

  @SuppressWarnings("unchecked")
  @Test
  public void testPresentSinkFieldsWithNullColumns()
    {
    String[] columnNames = new String[]{ "id", "firstname", "lastname" };
    JDBCScheme scheme = new JDBCScheme( DBInputFormat.class, DBOutputFormat.class, Fields.UNKNOWN, null, null, null, -1, null, null, null );

    @SuppressWarnings("rawtypes")
    Class[] fieldTypes = new Class<?>[]{ int.class, String.class, String.class };
    Fields fields = new Fields( columnNames, fieldTypes );
    FlowProcess<JobConf> fp = mock( FlowProcess.class );

    JDBCTap tap = mock( JDBCTap.class );

    TableDesc desc = new TableDesc( "test_table" );
    when( tap.getTableDesc() ).thenReturn( desc );

    scheme.presentSinkFields( fp, tap, fields );

    assertTrue( desc.hasRequiredTableInformation() );

    assertArrayEquals( columnNames, scheme.getColumns() );

    }

  @SuppressWarnings("unchecked")
  @Test(expected = IllegalArgumentException.class)
  public void testPresentSinkFieldsWithFieldsMismatch()
    {
    String[] columnNames = new String[]{ "id", "firstname", "lastname" };
    JDBCScheme scheme = new JDBCScheme( DBInputFormat.class, DBOutputFormat.class, Fields.UNKNOWN, columnNames, null, null, -1, null, null,
        null );

    @SuppressWarnings("rawtypes")
    Class[] fieldTypes = new Class<?>[]{ int.class, String.class };
    Fields fields = new Fields( new String[]{ "id", "firstname" }, fieldTypes );
    FlowProcess<JobConf> fp = mock( FlowProcess.class );

    JDBCTap tap = mock( JDBCTap.class );

    TableDesc desc = new TableDesc( "test_table" );
    when( tap.getTableDesc() ).thenReturn( desc );

    scheme.presentSinkFields( fp, tap, fields );

    }

  }
