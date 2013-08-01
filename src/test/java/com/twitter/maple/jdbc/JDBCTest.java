/*
 * Copyright (c) 2013 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package com.twitter.maple.jdbc;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.hsqldb.Server;
import org.junit.After;
import org.junit.Before;
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
 *
 */
public class JDBCTest
  {
  String inputFile = "src/test/resources/data/small.txt";
  private Server server;

  @Before
  public void setUp() throws IOException
    {
  	  File datadir = new File("build/db/testing");
  	  // the test hangs, if there are stale db file in the directory.
  	  if (datadir.exists())
  		  FileUtils.deleteDirectory(datadir);
    	
  	  server = new Server();
    	server.setDatabasePath( 0, datadir.getAbsolutePath() );
    	server.setDatabaseName( 0, "testing" );
    	server.start();
    }

  @After
  public void tearDown() throws IOException
    {
  		server.stop();
    }


  @Test
  public void testJDBC() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap source = new Hfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lwr", "upr" ), "\\s" ) );

    String url = "jdbc:hsqldb:hsql://localhost/testing";
    
    
    //String url = "jdbc:derby:memory:testing;create=true";
    String driver = "org.hsqldb.jdbcDriver";
    //String driver = "com.mysql.jdbc.Driver";
    //String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    String tableName = "testingtable";
    String[] columnNames = {"num", "lwr", "upr"};
    String[] columnDefs = {"VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"num", "lwr"};
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap replaceTap = new JDBCTap( url, driver, tableDesc, new JDBCScheme( columnNames ), SinkMode.REPLACE );

    Flow parseFlow = new HadoopFlowConnector(new Properties()).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    // create flow to read from hsqldb and save to local file
    Tap sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Properties props = new Properties();
    props.put("mapred.reduce.tasks.speculative.execution", "false");
    Flow copyFlow = new HadoopFlowConnector(props).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    JDBCScheme jdbcScheme = new JDBCScheme( columnNames, null, new String[]{"num", "lwr"} );
    Tap updateTap = new JDBCTap( url, driver, tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow updateFlow = new HadoopFlowConnector(props).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap sourceTap = new JDBCTap( url, driver, new JDBCScheme( columnNames,
            "select num, lwr, upr from testingtable as testingtable", "select count(*) from testingtable" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow readFlow = new HadoopFlowConnector(props).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }


  @Test
  public void testJDBCAliased() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap source = new Hfs( new TextLine(), inputFile );

    Fields columnFields = new Fields( "num", "lwr", "upr" );
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( columnFields, "\\s" ) );

    String url = "jdbc:hsqldb:hsql://localhost/testing";
    //String url = "jdbc:derby:memory:testing;create=true";
    String driver = "org.hsqldb.jdbcDriver";
    //String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    String tableName = "testingtablealias";
    String[] columnNames = {"db_num", "db_lower", "db_upper"};
    String[] columnDefs = {"VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"db_num", "db_lower"};
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap replaceTap = new JDBCTap( url, driver, tableDesc, new JDBCScheme( columnFields, columnNames ), SinkMode.REPLACE );

    Flow parseFlow = new HadoopFlowConnector(new Properties()).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    Tap sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new HadoopFlowConnector(new Properties()).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    Fields updateByFields = new Fields( "num", "lwr" );
    String[] updateBy = {"db_num", "db_lower"};
    JDBCScheme jdbcScheme = new JDBCScheme( columnFields, columnNames, null, updateByFields, updateBy );
    Tap updateTap = new JDBCTap( url, driver, tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow updateFlow = new HadoopFlowConnector(new Properties()).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap sourceTap = new JDBCTap( url, driver, new JDBCScheme( columnFields, columnNames,
            "select db_num, db_lower, db_upper from testingtablealias as testingtablealias", "select count(*) from testingtablealias" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow readFlow = new HadoopFlowConnector().connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }

  private void verifySink( Flow flow, int expects ) throws IOException
    {
    int count = 0;

    TupleEntryIterator iterator = flow.openSink();

    while( iterator.hasNext() )
      {
      count++;
      System.out.println( "iterator.next() = " + iterator.next() );
      }

    iterator.close();

    assertEquals( "wrong number of values", expects, count );
    }

  }

