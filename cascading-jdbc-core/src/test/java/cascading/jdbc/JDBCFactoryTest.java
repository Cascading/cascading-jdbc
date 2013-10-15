/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Test;

import cascading.jdbc.JDBCFactory;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.jdbc.db.DBInputFormat;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;

/**
 * Tests for {@link JDBCFactory}.
 * 
 * */
public class JDBCFactoryTest
  {

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoTableName()
    {
    String protocol = "jdbc";
    String identifier = "jdbc:some:stuf//database";
    JDBCScheme mockScheme = mock( JDBCScheme.class );

    JDBCFactory factory = new JDBCFactory();

    Properties props = new Properties();
    props.setProperty( JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_USER, "username" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password" );

    factory.createTap( protocol, mockScheme, identifier, SinkMode.REPLACE, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapEmptyTableName()
    {
    String protocol = "jdbc";
    String identifier = "jdbc:some:stuf//database";
    JDBCScheme mockScheme = mock( JDBCScheme.class );

    JDBCFactory factory = new JDBCFactory();

    Properties props = new Properties();
    props.setProperty( JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_USER, "username" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password" );
    props.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "" );

    factory.createTap( protocol, mockScheme, identifier, SinkMode.REPLACE, props );
    }


  @Test
  public void testCreateTapFullyWorking()
    {
    String protocol = "jdbc";
    String identifier = "jdbc:some:stuf//database";
    JDBCScheme mockScheme = mock( JDBCScheme.class );

    JDBCFactory factory = new JDBCFactory();

    Properties props = new Properties();
    props.setProperty( JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_USER, "username" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password" );

    props.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "myTable" );
    props.setProperty( JDBCFactory.PROTOCOL_COLUMN_NAMES, "id:name:lastname" );

    props.setProperty( JDBCFactory.PROTOCOL_COLUMN_DEFS, "int:varchar(42):varchar(23)" );
    props.setProperty( JDBCFactory.PROTOCOL_PRIMARY_KEYS, "id" );

    JDBCTap tap = (JDBCTap) factory.createTap( protocol, mockScheme, identifier, SinkMode.REPLACE, props );
    assertEquals( mockScheme, tap.getScheme() );
    assertEquals( "myTable", tap.getTableName() );
    assertEquals( SinkMode.REPLACE, tap.getSinkMode() );
    TableDesc tdesc = tap.tableDesc;

    assertEquals( "myTable", tdesc.getTableName() );
    assertArrayEquals( new String[] { "id", "name", "lastname" }, tdesc.getColumnNames() );
    assertArrayEquals( new String[] { "int", "varchar(42)", "varchar(23)" }, tdesc.getColumnDefs() );
    assertArrayEquals( new String[] { "id" }, tdesc.getPrimaryKeys() );

    }
  
  @Test
  public void testCreateTapSinkModeOverwrite()
    {
    String protocol = "jdbc";
    String identifier = "jdbc:some:stuf//database";
    JDBCScheme mockScheme = mock( JDBCScheme.class );

    JDBCFactory factory = new JDBCFactory();

    Properties props = new Properties();
    props.setProperty( JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_USER, "username" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password" );

    props.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "myTable" );
    props.setProperty( JDBCFactory.PROTOCOL_COLUMN_NAMES, "id:name:lastname" );

    props.setProperty( JDBCFactory.PROTOCOL_COLUMN_DEFS, "int:varchar(42):varchar(23)" );
    props.setProperty( JDBCFactory.PROTOCOL_PRIMARY_KEYS, "id" );
    props.setProperty( JDBCFactory.PROTOCOL_SINK_MODE, "KEEP" );


    JDBCTap tap = (JDBCTap) factory.createTap( protocol, mockScheme, identifier, SinkMode.REPLACE, props );
    assertEquals( mockScheme, tap.getScheme() );
    assertEquals( "myTable", tap.getTableName() );
    assertEquals( SinkMode.KEEP, tap.getSinkMode() );
    TableDesc tdesc = tap.tableDesc;

    assertEquals( "myTable", tdesc.getTableName() );
    assertArrayEquals( new String[] { "id", "name", "lastname" }, tdesc.getColumnNames() );
    assertArrayEquals( new String[] { "int", "varchar(42)", "varchar(23)" }, tdesc.getColumnDefs() );
    assertArrayEquals( new String[] { "id" }, tdesc.getPrimaryKeys() );

    }

  @Test
  public void testCreateTapFullyWorkingWithEmptyUserAndPass()
    {
    String protocol = "jdbc";
    String identifier = "jdbc:some:stuf//database";
    JDBCScheme mockScheme = mock( JDBCScheme.class );

    JDBCFactory factory = new JDBCFactory();

    Properties props = new Properties();
    props.setProperty( JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_USER, "" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_PASSWORD, "" );

    props.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "myTable" );
    props.setProperty( JDBCFactory.PROTOCOL_COLUMN_NAMES, "id:name:lastname" );

    props.setProperty( JDBCFactory.PROTOCOL_COLUMN_DEFS, "int:varchar(42):varchar(23)" );
    props.setProperty( JDBCFactory.PROTOCOL_PRIMARY_KEYS, "id" );

    JDBCTap tap = (JDBCTap) factory.createTap( protocol, mockScheme, identifier, SinkMode.UPDATE, props );
    assertEquals( mockScheme, tap.getScheme() );
    assertEquals( "myTable", tap.getTableName() );
    assertEquals( SinkMode.UPDATE, tap.getSinkMode() );
    TableDesc tdesc = tap.tableDesc;

    assertEquals( "myTable", tdesc.getTableName() );
    assertArrayEquals( new String[] { "id", "name", "lastname" }, tdesc.getColumnNames() );
    assertArrayEquals( new String[] { "int", "varchar(42)", "varchar(23)" }, tdesc.getColumnDefs() );
    assertArrayEquals( new String[] { "id" }, tdesc.getPrimaryKeys() );

    }

  @Test
  public void testCreateTapWithMissingTableInformation()
    {
    String protocol = "jdbc";
    String identifier = "jdbc:some:stuf//database";
    JDBCScheme mockScheme = mock( JDBCScheme.class );

    JDBCFactory factory = new JDBCFactory();

    Properties props = new Properties();
    props.setProperty( JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_USER, "" );
    props.setProperty( JDBCFactory.PROTOCOL_JDBC_PASSWORD, "" );

    props.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "myTable" );
    
    String[] columnNames = new String [] {"id", "name", "lastname"};
    @SuppressWarnings("rawtypes")
    Class[] fieldTypes = new Class<?>[] {int.class, String.class, String.class};
    
    Fields fields = new Fields(columnNames, fieldTypes);
    when(mockScheme.getSinkFields()).thenReturn( fields );
    
    JDBCTap tap = (JDBCTap) factory.createTap( protocol, mockScheme, identifier, SinkMode.UPDATE, props );
    
    TableDesc tdesc = tap.tableDesc;
    assertEquals( "myTable", tdesc.getTableName() );
    assertArrayEquals( new String[] { "id", "name", "lastname" }, tdesc.getColumnNames() );
    assertArrayEquals( new String[] { "int not null", "varchar(256)", "varchar(256)" }, tdesc.getColumnDefs() );

    }
  
  @Test
  public void testCreateScheme()
    {
    JDBCFactory factory = new JDBCFactory();
    Fields fields = new Fields( "ONE", "TWO", "THREE" );

    Properties schemeProperties = new Properties();
    schemeProperties.setProperty( JDBCFactory.FORMAT_COLUMNS, "one:two:three" );

    Scheme<?, ?, ?, ?, ?> scheme = factory.createScheme( "someFormat", fields, schemeProperties );
    assertNotNull( scheme );

    JDBCScheme jdbcScheme = (JDBCScheme) scheme;

    assertArrayEquals( jdbcScheme.getColumns(), new String[] { "one", "two", "three" } );

    }
  
  
  @Test
  public void testCreateSchemeColumnsFromFields()
    {
    JDBCFactory factory = new JDBCFactory();
    Fields fields = new Fields( "one", "two", "three" );

    Properties schemeProperties = new Properties();

    Scheme<?, ?, ?, ?, ?> scheme = factory.createScheme( "someFormat", fields, schemeProperties );
    assertNotNull( scheme );

    JDBCScheme jdbcScheme = (JDBCScheme) scheme;

    assertArrayEquals( jdbcScheme.getColumns(), new String[] { "one", "two", "three" } );

    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeWithSelectNoCount()
    {
    JDBCFactory factory = new JDBCFactory();
    Fields fields = new Fields( "one", "two", "three" );

    Properties schemeProperties = new Properties();
    schemeProperties.setProperty( JDBCFactory.FORMAT_COLUMNS, "one:two:three" );
    schemeProperties.setProperty( JDBCFactory.FORMAT_SELECT_QUERY, "select one, two, three from table" );

    factory.createScheme( "someFormat", fields, schemeProperties );
    }

  @Test
  public void testCreateSchemeWithSelectAndCount()
    {
    JDBCFactory factory = new JDBCFactory();
    Fields fields = new Fields( "one", "two", "three" );

    Properties schemeProperties = new Properties();
    schemeProperties.setProperty( JDBCFactory.FORMAT_COLUMNS, "one:two:three" );
    schemeProperties.setProperty( JDBCFactory.FORMAT_SELECT_QUERY, "select one, two, three from table" );
    schemeProperties.setProperty( JDBCFactory.FORMAT_COUNT_QUERY, "select count(*) from table" );

    Scheme<?, ?, ?, ?, ?> scheme = factory.createScheme( "someFormat", fields, schemeProperties );
    assertNotNull( scheme );
    }
  
  @Test
  public void testGetInputFormatClass()
    {
    assertEquals(DBInputFormat.class, new JDBCFactory().getInputFormatClass());
    }

  }
