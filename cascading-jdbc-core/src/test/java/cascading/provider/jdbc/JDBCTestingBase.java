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

package cascading.provider.jdbc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.provider.jdbc.JDBCScheme;
import cascading.provider.jdbc.JDBCTap;
import cascading.provider.jdbc.TableDesc;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

/**
 * Base class for the various database tests. This class contains the actual tests, while the subclasses
 * can implement their own specific setUp and tearDown methods.
 */
public abstract class JDBCTestingBase
  {
  String inputFile = "../cascading-jdbc-core/src/test/resources/data/small.txt";

  /** the JDBC url for the tests. subclasses have to set this */
  private String jdbcurl;

	/** the name of the JDBC driver to use.*/
  private String driverName;

  @Test
  public void testJDBC() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lwr", "upr" ), "\\s" ) );

    String tableName = "testingtable";
    String[] columnNames = {"num", "lwr", "upr"};
    String[] columnDefs = {"VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"num", "lwr"};
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap<?, ?, ?> replaceTap = new JDBCTap( jdbcurl, driverName, tableDesc, new JDBCScheme( columnNames ), SinkMode.REPLACE );

    Flow<?> parseFlow = new HadoopFlowConnector(createProperties()).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    // create flow to read from hsqldb and save to local file
    Tap<?, ?, ?> sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow<?> copyFlow = new HadoopFlowConnector(createProperties()).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    JDBCScheme jdbcScheme = new JDBCScheme( columnNames, null, new String[]{"num", "lwr"} );
    Tap<?, ?, ?> updateTap = new JDBCTap( jdbcurl, driverName, tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow<?> updateFlow = new HadoopFlowConnector(createProperties()).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap<?, ?, ?> sourceTap = new JDBCTap( jdbcurl, driverName, new JDBCScheme( columnNames,
            "select num, lwr, upr from testingtable as testingtable", "select count(*) from testingtable" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow<?> readFlow = new HadoopFlowConnector(createProperties()).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }


  @Test
  public void testJDBCAliased() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );
    Fields columnFields = new Fields( "num", "lwr", "upr" );
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( columnFields, "\\s" ) );


    String tableName = "testingtablealias";
    String[] columnNames = {"db_num", "db_lower", "db_upper"};
    String[] columnDefs = {"VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"db_num", "db_lower"};
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap<?, ?, ?> replaceTap = new JDBCTap( jdbcurl, driverName, tableDesc, new JDBCScheme( columnFields, columnNames ), SinkMode.REPLACE );

    Flow<?> parseFlow = new HadoopFlowConnector(createProperties()).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    Tap<?, ?, ?> sink = new Hfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow<?> copyFlow = new HadoopFlowConnector(createProperties()).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    Fields updateByFields = new Fields( "num", "lwr" );
    String[] updateBy = {"db_num", "db_lower"};
    JDBCScheme jdbcScheme = new JDBCScheme( columnFields, columnNames, null, updateByFields, updateBy );
    Tap<?, ?, ?> updateTap = new JDBCTap( jdbcurl, driverName, tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow<?> updateFlow = new HadoopFlowConnector(createProperties()).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap<?, ?, ?> sourceTap = new JDBCTap( jdbcurl, driverName, new JDBCScheme( columnFields, columnNames,
            "select db_num, db_lower, db_upper from testingtablealias as testingtable", "select count(*) from testingtablealias" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow<?> readFlow = new HadoopFlowConnector(createProperties()).connect( sourceTap, sink, readPipe );

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
     props.put("mapred.reduce.tasks.speculative.execution", "false");
     props.put("mapred.map.tasks.speculative.execution", "false");
     return props;
     }
   
   
  public void setJdbcurl(String jdbcurl)
    {
 		this.jdbcurl = jdbcurl;
 	  }

 	public void setDriverName(String driverName)
 	  {
    this.driverName = driverName;
 	  }
   
  }
