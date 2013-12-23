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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.DBWritable;
import cascading.lingual.catalog.provider.ProviderDefinition;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Base class for the various database tests. This class contains the actual
 * tests, while the subclasses can implement their own specific setUp and
 * tearDown methods.
 */
public abstract class JDBCTestingBase
  {
  String inputFile = "../cascading-jdbc-core/src/test/resources/data/small.txt";

  /** the JDBC url for the tests. subclasses have to set this */
  protected String jdbcurl;

  /** the name of the JDBC driver to use. */
  protected String driverName;

  /** The input format class to use. */
  protected Class<? extends DBInputFormat> inputFormatClass = DBInputFormat.class;

  /** The jdbc factory to use. */
  private JDBCFactory factory = new JDBCFactory();

  protected org.slf4j.Logger LOG = LoggerFactory.getLogger( getClass() );

  protected static final String TESTING_TABLE_NAME = "testingtable";

  @Test
  public void testJDBC() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap<?, ?, ?> source = new Hfs( new TextLine(), inputFile );
    Fields fields = new Fields( new Comparable[]{"num", "lwr", "upr"}, new Type[]{int.class, String.class,
                                                                                  String.class} );
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( fields, "\\s" ) );

    String[] columnNames = {"num", "lwr", "upr"};
    String[] columnDefs = {"INT NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"num", "lwr"};
    TableDesc tableDesc = getNewTableDesc( TESTING_TABLE_NAME, columnNames, columnDefs, primaryKeys );

    JDBCScheme scheme = getNewJDBCScheme( fields, columnNames );

    Tap<?, ?, ?> replaceTap = getNewJDBCTap( tableDesc, scheme, SinkMode.REPLACE );

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

    JDBCScheme jdbcScheme = getNewJDBCScheme( columnNames, null, new String[]{"num", "lwr"} );
    jdbcScheme.setSinkFields( fields );
    Tap<?, ?, ?> updateTap = getNewJDBCTap( tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow<?> updateFlow = new HadoopFlowConnector( createProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();
    updateFlow.cleanup();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap<?, ?, ?> sourceTap = getNewJDBCTap( getNewJDBCScheme( columnNames,
      String.format( "select num, lwr, upr from %s %s", TESTING_TABLE_NAME, TESTING_TABLE_NAME), "select count(*) from " + TESTING_TABLE_NAME ) );

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
    Fields fields = new Fields( new Comparable[]{"num", "lwr", "upr"}, new Type[]{int.class, String.class,
                                                                                  String.class} );
    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( fields, "\\s" ) );

    String tableAlias = TESTING_TABLE_NAME + "alias";
    String[] columnNames = {"num", "lwr", "upr"};
    String[] columnDefs = {"INT NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"num", "lwr"};
    TableDesc tableDesc = getNewTableDesc( tableAlias, columnNames, columnDefs, primaryKeys );

    Tap<?, ?, ?> replaceTap = getNewJDBCTap( tableDesc, getNewJDBCScheme( fields, columnNames ), SinkMode.REPLACE );

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

    JDBCScheme jdbcScheme = getNewJDBCScheme( columnNames, null, new String[]{"num", "lwr"} );
    jdbcScheme.setSinkFields( fields );
    Tap<?, ?, ?> updateTap = getNewJDBCTap( tableDesc, jdbcScheme, SinkMode.UPDATE );

    Flow<?> updateFlow = new HadoopFlowConnector( createProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap<?, ?, ?> sourceTap = getNewJDBCTap( getNewJDBCScheme( columnNames,
      String.format( "select num, lwr, upr from %s %s", tableAlias, TESTING_TABLE_NAME), "select count(*) from " + tableAlias ) );

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

    Fields columnFields = new Fields( new Comparable[]{"num", "lwr", "upr"}, new Type[]{int.class, String.class,
                                                                                        String.class} );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( columnFields, "\\s" ) );

    Properties tapProperties = new Properties();
    tapProperties.setProperty( JDBCFactory.PROTOCOL_COLUMN_DEFS, "int not null:varchar(100) not null: varchar(100) not null" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_COLUMN_NAMES, "num:lwr:upr" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_PRIMARY_KEYS, "num:lwr" );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, TESTING_TABLE_NAME );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, driverName );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_TABLE_EXISTS_QUERY, getTableExistsQuery() );

    String[] columnNames = new String[]{"num", "lwr", "upr"};

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

    Tap<?, ?, ?> updateTap = factory.createTap( "jdbc", updateScheme, jdbcurl, getSinkModeForReset(), tapProperties );

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

    Fields columnFields = new Fields( new Comparable[]{"num", "lwr", "upr"}, new Type[]{int.class, String.class,
                                                                                        String.class} );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( columnFields, "\\s" ) );

    Properties tapProperties = new Properties();
    tapProperties.setProperty( JDBCFactory.PROTOCOL_TABLE_NAME, TESTING_TABLE_NAME );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_JDBC_DRIVER, driverName );
    tapProperties.setProperty( JDBCFactory.PROTOCOL_TABLE_EXISTS_QUERY, getTableExistsQuery() );

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

    Tap<?, ?, ?> updateTap = factory.createTap( "jdbc", updateScheme, jdbcurl, getSinkModeForReset(), tapProperties );

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

  protected JDBCScheme getNewJDBCScheme( Fields fields, String[] columnNames )
    {
    return new JDBCScheme( inputFormatClass, fields, columnNames );
    }

  protected JDBCScheme getNewJDBCScheme( String[] columns, String[] orderBy, String[] updateBy )
    {
    return new JDBCScheme( columns, orderBy, updateBy );
    }

  protected JDBCScheme getNewJDBCScheme( String[] columnsNames, String contentsQuery, String countStarQuery )
    {
    return new JDBCScheme( columnsNames, contentsQuery, countStarQuery );
    }

  protected TableDesc getNewTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys )
    {
    return new TableDesc( tableName, columnNames, columnDefs, primaryKeys, getTableExistsQuery() );
    }

  protected JDBCTap getNewJDBCTap( TableDesc tableDesc, JDBCScheme jdbcScheme, SinkMode sinkMode )
    {
    return new JDBCTap( jdbcurl, driverName, tableDesc, jdbcScheme, sinkMode );
    }

  protected JDBCTap getNewJDBCTap( JDBCScheme jdbcScheme )
    {
    return new JDBCTap( jdbcurl, driverName, jdbcScheme );
    }

  /**
   * SinkMode.UPDATE is not intended for production use so allow data sources that have different semantics for data
   * swapping to override this.
   */
  protected SinkMode getSinkModeForReset() {
    return SinkMode.UPDATE;
  }


  protected Properties createProperties()
    {
    Properties props = new Properties();
    props.put( "mapred.reduce.tasks.speculative.execution", "false" );
    props.put( "mapred.map.tasks.speculative.execution", "false" );
    AppProps.setApplicationJarClass( props, getClass() );
    LOG.info( "running with properties {}", props);
    return props;
    }

  public String getTableExistsQuery() {

  Enumeration<URL> resources = null;
  try
    {
    resources = this.getClass().getClassLoader().getResources( ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES );
    URL url = resources.nextElement();
    LOG.debug( "loading properties from: {}", url );
    InputStream inputStream = url.openStream();
    Properties properties = new Properties();
    properties.load( inputStream );
    inputStream.close();
    String tableExistsQuery = JDBCFactory.DEFAULT_TABLE_EXISTS_QUERY;
    Set<String> keySet = properties.stringPropertyNames();
    for (String keyName : keySet)
      if (keyName.toString().endsWith( JDBCFactory.PROTOCOL_TABLE_EXISTS_QUERY ))
        tableExistsQuery = properties.getProperty( keyName ) ;

    return tableExistsQuery;
    }
  catch( IOException exception )
    {
    LOG.error( "unable to read {}. using default query", ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES, exception );
    return JDBCFactory.DEFAULT_TABLE_EXISTS_QUERY;
    }
  }

  public void setJdbcurl( String jdbcurl )
    {
    this.jdbcurl = jdbcurl;
    }

  public void setDriverName( String driverName )
    {
    this.driverName = driverName;
    }

  public void setJDBCFactory( JDBCFactory factory )
    {
    this.factory = factory;
    }

  public void setInputFormatClass( Class<? extends DBInputFormat<DBWritable>> inputFormatClass )
    {
    this.inputFormatClass = inputFormatClass;
    }

  public void setFactory( JDBCFactory factory )
    {
    this.factory = factory;
    }
  }
