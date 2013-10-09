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

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

/**
 * Base class for the various database tests. This class contains the actual
 * tests, while the subclasses can implement their own specific setUp and
 * tearDown methods.
 */
public abstract class JDBCTestingBase
  {
  String inputFile = "../cascading-jdbc-core/src/test/resources/data/small.txt";

  /** the JDBC url for the tests. subclasses have to set this */
  private String jdbcurl;

  /** the name of the JDBC driver to use. */
  private String driverName;

  @Test
  public void testJDBC() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );
    Fields fields = new Fields( new Comparable[]{ "num", "lwr", "upr" }, new Type[]{ int.class, String.class, String.class } );
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( fields, "\\s" ) );

    String tableName = "testingtable";
    String[] columnNames = { "num", "lwr", "upr" };
    String[] columnDefs = { "INT NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL" };
    String[] primaryKeys = { "num", "lwr" };
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap<?, ?, ?> replaceTap = new JDBCTap( jdbcurl, driverName, tableDesc, new JDBCScheme( fields, columnNames ), SinkMode.REPLACE );

    Flow<?> parseFlow = new HadoopFlowConnector( createProperties() ).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    Tap<?, ?, ?> sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow<?> copyFlow = new HadoopFlowConnector( createProperties() ).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    JDBCScheme jdbcScheme = new JDBCScheme( columnNames, null, new String[]{ "num", "lwr" } );
    jdbcScheme.setSinkFields( fields );
    Tap<?, ?, ?> updateTap = new JDBCTap( jdbcurl, driverName, tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow<?> updateFlow = new HadoopFlowConnector( createProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap<?, ?, ?> sourceTap = new JDBCTap( jdbcurl, driverName, new JDBCScheme( columnNames,
        "select num, lwr, upr from testingtable testingtable", "select count(*) from testingtable" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow<?> readFlow = new HadoopFlowConnector( createProperties() ).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }

  @Test
  public void testJDBCAliased() throws IOException
    {
    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );
    Fields fields = new Fields( new Comparable[]{ "num", "lwr", "upr" }, new Type[]{ int.class, String.class, String.class } );
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( fields, "\\s" ) );

    String tableName = "testingtablealias";
    String[] columnNames = { "num", "lwr", "upr" };
    String[] columnDefs = { "INT NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL" };
    String[] primaryKeys = { "num", "lwr" };
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap<?, ?, ?> replaceTap = new JDBCTap( jdbcurl, driverName, tableDesc, new JDBCScheme(fields, columnNames ), SinkMode.REPLACE );

    Flow<?> parseFlow = new HadoopFlowConnector( createProperties() ).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    Tap<?, ?, ?> sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow<?> copyFlow = new HadoopFlowConnector( createProperties() ).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    JDBCScheme jdbcScheme = new JDBCScheme( columnNames, null, new String[]{ "num", "lwr" } );
    jdbcScheme.setSinkFields( fields );
    Tap<?, ?, ?> updateTap = new JDBCTap( jdbcurl, driverName, tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow<?> updateFlow = new HadoopFlowConnector( createProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap<?, ?, ?> sourceTap = new JDBCTap( jdbcurl, driverName, new JDBCScheme( columnNames,
        "select num, lwr, upr from testingtablealias testingtable", "select count(*) from testingtablealias" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow<?> readFlow = new HadoopFlowConnector( createProperties() ).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    // CREATE NEW TABLE FROM SOURCE

    }

  @Test
  public void testJDBCWithFactory() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );

    Fields columnFields = new Fields( new Comparable[]{ "num", "lwr", "upr" }, new Type[]{ int.class, String.class, String.class } );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( columnFields, "\\s" ) );

    Properties tapProperties = new Properties();
    tapProperties.setProperty( JDBCFactory.PROTOCOL_COLUMN_DEFS, "int not null:varchar(100) not null: varchar(100) not null" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_COLUMN_NAMES, "num:lwr:upr" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_PRIMARY_KEYS, "num:lwr" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "testingtable" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, driverName );

    String[] columnNames = new String[]{ "num", "lwr", "upr" };

    JDBCFactory factory = new JDBCFactory();

    Properties schemeProperties = new Properties();
    schemeProperties.setProperty( JDBCFactory.FORMAT_COLUMNS, StringUtils.join( columnNames, ":" ) );
    JDBCScheme scheme = (JDBCScheme) factory.createScheme( "somename", columnFields, schemeProperties );

    Tap<?, ?, ?> replaceTap = factory.createTap( "jdbc", scheme, jdbcurl, SinkMode.REPLACE, tapProperties );

    Flow<?> parseFlow = new HadoopFlowConnector( createProperties() ).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    Tap<?, ?, ?> sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow<?> copyFlow = new HadoopFlowConnector( createProperties() ).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    schemeProperties.put( JDBCFactory.FORMAT_UPDATE_BY, "num:lwr" );

    JDBCScheme updateScheme = (JDBCScheme) factory.createScheme( "somename", columnFields, schemeProperties );

    Tap<?, ?, ?> updateTap = factory.createTap( "jdbc", updateScheme, jdbcurl, SinkMode.UPDATE, tapProperties );

    Flow<?> updateFlow = new HadoopFlowConnector( createProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    schemeProperties.remove( JDBCFactory.FORMAT_UPDATE_BY );

    JDBCScheme sourceScheme = (JDBCScheme) factory.createScheme( "somename", columnFields, schemeProperties );
    Tap<?, ?, ?> sourceTap = factory.createTap( "jdbc", sourceScheme, jdbcurl, SinkMode.KEEP, tapProperties );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow<?> readFlow = new HadoopFlowConnector( createProperties() ).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }

  @Test
  public void testJDBCWithFactoryMissingTypes() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );

    Fields columnFields = new Fields( new Comparable[]{ "num", "lwr", "upr" }, new Type[]{ int.class, String.class, String.class } );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( columnFields, "\\s" ) );

    Properties tapProperties = new Properties();
    tapProperties.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, "testingtable" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, driverName );

    JDBCFactory factory = new JDBCFactory();

    Properties schemeProperties = new Properties();
    JDBCScheme scheme = (JDBCScheme) factory.createScheme( "somename", columnFields, schemeProperties );

    Tap<?, ?, ?> replaceTap = factory.createTap( "jdbc", scheme, jdbcurl, SinkMode.REPLACE, tapProperties );

    Flow<?> parseFlow = new HadoopFlowConnector( createProperties() ).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    Tap<?, ?, ?> sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow<?> copyFlow = new HadoopFlowConnector( createProperties() ).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    schemeProperties.put( JDBCFactory.FORMAT_UPDATE_BY, "num:lwr" );

    JDBCScheme updateScheme = (JDBCScheme) factory.createScheme( "somename", columnFields, schemeProperties );

    Tap<?, ?, ?> updateTap = factory.createTap( "jdbc", updateScheme, jdbcurl, SinkMode.UPDATE, tapProperties );

    Flow<?> updateFlow = new HadoopFlowConnector( createProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    schemeProperties.remove( JDBCFactory.FORMAT_UPDATE_BY );

    JDBCScheme sourceScheme = (JDBCScheme) factory.createScheme( "somename", columnFields, schemeProperties );
    Tap<?, ?, ?> sourceTap = factory.createTap( "jdbc", sourceScheme, jdbcurl, SinkMode.KEEP, tapProperties );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow<?> readFlow = new HadoopFlowConnector( createProperties() ).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }

  private void verifySink( Flow<?> flow, int expects ) throws IOException
    {
    int count = 0;

    TupleEntryIterator iterator = flow.openSink();

    while( iterator.hasNext() )
      {
      count++;
      iterator.next();
      }

    iterator.close();

    assertEquals( "wrong number of values", expects, count );
    }

  private Properties createProperties()
    {
    Properties props = new Properties();
    props.put( "mapred.reduce.tasks.speculative.execution", "false" );
    props.put( "mapred.map.tasks.speculative.execution", "false" );
    return props;
    }

  public void setJdbcurl( String jdbcurl )
    {
    this.jdbcurl = jdbcurl;
    }

  public void setDriverName( String driverName )
    {
    this.driverName = driverName;
    }

  }
