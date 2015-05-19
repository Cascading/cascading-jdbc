/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.jdbc.db.DBConfiguration;
import cascading.management.annotation.URISanitizer;
import cascading.property.AppProps;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class JDBCTap is a {@link Tap} sub-class that provides read and write access
 * to a RDBMS via JDBC drivers.
 * <p/>
 * This Tap fully supports TABLE DROP and CREATE when given a {@link TableDesc}
 * instance.
 * <p/>
 * When using {@link SinkMode#UPDATE}, Cascading is instructed to not delete the
 * resource (drop the Table) and assumes its safe to begin sinking data into it.
 * The {@link JDBCScheme} is responsible for deciding if/when to perform an
 * UPDATE instead of an INSERT.
 * <p/>
 * Both INSERT and UPDATE are supported through the JDBCScheme.
 * <p/>
 * By sub-classing JDBCScheme, {@link cascading.jdbc.db.DBInputFormat}, and
 * {@link cascading.jdbc.db.DBOutputFormat}, specific vendor features can be
 * supported.
 * <p/>
 * Use {@link #setBatchSize(int)} to set the number of INSERT/UPDATES should be
 * grouped together before being executed. The default vaue is 1,000.
 * <p/>
 * Use {@link #executeQuery(String, int)} or {@link #executeUpdate(String)} to
 * invoke SQL statements against the underlying Table.
 * <p/>
 * Note that all classes under the <pre>cascading.jdbc.db</pre> package originated
 * from the Hadoop project and retain their Apache 2.0 license though they have
 * been heavily modified to support INSERT/UPDATE and vendor specialization, and
 * a number of other features like 'limit'.
 *
 * @see JDBCScheme
 * @see cascading.jdbc.db.DBInputFormat
 * @see cascading.jdbc.db.DBOutputFormat
 */
public class JDBCTap extends Tap<Configuration, RecordReader, OutputCollector>
  {
  static
    {
    // make sure usernames and passwords are sanitized before they are sent to the DocumentService.
    StringBuilder parametersToFiler = new StringBuilder( "user,password" );
    if( System.getProperty( URISanitizer.PARAMETER_FILTER_PROPERTY ) != null )
      parametersToFiler.append( "," ).append( System.getProperty( URISanitizer.PARAMETER_FILTER_PROPERTY ) );
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, parametersToFiler.toString() );

    // add cascading-jdbc release to frameworks
    Properties properties = new Properties();
    InputStream stream = JDBCTap.class.getClassLoader().getResourceAsStream( "cascading/framework.properties" );
    if( stream != null )
      {
      try
        {
        properties.load( stream );
        stream.close();
        }
      catch( IOException exception )
        {
        // ingore
        }
      }
    String framework = properties.getProperty( "name" );
    AppProps.addApplicationFramework( null, framework );
    }

  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( JDBCTap.class );

  /** unique identifier */
  private final String id = UUID.randomUUID().toString();

  /** Field connectionUrl */
  String connectionUrl;
  /** Field username */
  String username;
  /** Field password */
  String password;
  /** Field driverClassName */
  String driverClassName;
  /** Field tableDesc */
  TableDesc tableDesc;
  /** Field batchSize */
  int batchSize = 1000;
  /** Field concurrentReads */
  int concurrentReads = 0;

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   * <p/>
   * Use this constructor for connecting to existing tables that will be read
   * from, or will be inserted/updated into. By default it uses
   * {@link SinkMode#UPDATE}.
   *
   * @param connectionUrl of type String
   * @param username of type String
   * @param password of type String
   * @param driverClassName of type String
   * @param tableName of type String
   * @param scheme of type JDBCScheme
   */
  public JDBCTap( String connectionUrl, String username, String password, String driverClassName, String tableName, JDBCScheme scheme )
    {
    this( connectionUrl, username, password, driverClassName, new TableDesc( tableName ), scheme, SinkMode.UPDATE );
    }

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   *
   * @param connectionUrl of type String
   * @param driverClassName of type String
   * @param tableDesc of type TableDesc
   * @param scheme of type JDBCScheme
   * @param sinkMode of type SinkMode
   */
  public JDBCTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme scheme, SinkMode sinkMode )
    {
    this( connectionUrl, null, null, driverClassName, tableDesc, scheme, sinkMode );
    }


  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   * <p/>
   * Use this constructor for connecting to existing tables that will be read
   * from, or will be inserted/updated into. By default it uses
   * {@link SinkMode#UPDATE}.
   *
   * @param connectionUrl of type String
   * @param username of type String
   * @param password of type String
   * @param driverClassName of type String
   * @param tableDesc of type TableDesc
   * @param scheme of type JDBCScheme
   */
  public JDBCTap( String connectionUrl, String username, String password, String driverClassName, TableDesc tableDesc, JDBCScheme scheme )
    {
    this( connectionUrl, username, password, driverClassName, tableDesc, scheme, SinkMode.UPDATE );
    }

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   *
   * @param connectionUrl of type String
   * @param username of type String
   * @param password of type String
   * @param driverClassName of type String
   * @param tableDesc of type TableDesc
   * @param scheme of type JDBCScheme
   * @param sinkMode of type SinkMode
   */
  public JDBCTap( String connectionUrl, String username, String password, String driverClassName, TableDesc tableDesc, JDBCScheme scheme,
      SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.connectionUrl = connectionUrl;
    this.username = username;
    this.password = password;
    this.driverClassName = driverClassName;
    this.tableDesc = tableDesc;

    if ( sinkMode != SinkMode.UPDATE && sinkMode != SinkMode.KEEP )
      LOG.warn( "using sink mode: {}, consider UPDATE to prevent DROP TABLE from being called during Flow or Cascade setup", sinkMode );
    }

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   * <p/>
   * Use this constructor for connecting to existing tables that will be read
   * from, or will be inserted/updated into. By default it uses
   * {@link SinkMode#UPDATE}.
   *
   * @param connectionUrl of type String
   * @param driverClassName of type String
   * @param tableDesc of type TableDesc
   * @param scheme of type JDBCScheme
   */
  public JDBCTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme scheme )
    {
    this( connectionUrl, driverClassName, tableDesc, scheme, SinkMode.UPDATE );
    }

  /**
   * Constructor JDBCTap creates a new JDBCTap instance that may only used as a
   * data source.
   *
   * @param connectionUrl of type String
   * @param username of type String
   * @param password of type String
   * @param driverClassName of type String
   * @param scheme of type JDBCScheme
   */
  public JDBCTap( String connectionUrl, String username, String password, String driverClassName, JDBCScheme scheme )
    {
    super( scheme );
    this.connectionUrl = connectionUrl;
    this.username = username;
    this.password = password;
    this.driverClassName = driverClassName;
    }

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   *
   * @param connectionUrl of type String
   * @param driverClassName of type String
   * @param scheme of type JDBCScheme
   */
  public JDBCTap( String connectionUrl, String driverClassName, JDBCScheme scheme )
    {
    this( connectionUrl, null, null, driverClassName, scheme );
    }

  /**
   * Method getTableName returns the tableName of this JDBCTap object.
   *
   * @return the tableName (type String) of this JDBCTap object.
   */
  public String getTableName()
    {
    return tableDesc.tableName;
    }

  /**
   * Method setBatchSize sets the batchSize of this JDBCTap object.
   *
   * @param batchSize the batchSize of this JDBCTap object.
   */
  public void setBatchSize( int batchSize )
    {
    this.batchSize = batchSize;
    }

  /**
   * Method getBatchSize returns the batchSize of this JDBCTap object.
   *
   * @return the batchSize (type int) of this JDBCTap object.
   */
  public int getBatchSize()
    {
    return batchSize;
    }

  /**
   * Method getTableDesc returns the {@link TableDesc} of this {@link JDBCTap}.
   *
   * @return the table description
   */
  public TableDesc getTableDesc()
    {
    return tableDesc;
    }

  /**
   * Method getConcurrentReads returns the concurrentReads of this JDBCTap
   * object.
   * <p/>
   * This value specifies the number of concurrent selects and thus the number
   * of mappers that may be used. A value of -1 uses the job default.
   *
   * @return the concurrentReads (type int) of this JDBCTap object.
   */
  @Deprecated
  public int getConcurrentReads()
    {
    return concurrentReads;
    }

  /**
   * Method setConcurrentReads sets the concurrentReads of this JDBCTap object.
   * <p/>
   * This value specifies the number of concurrent selects and thus the number
   * of mappers that may be used. A value of -1 uses the job default.
   *
   * @param concurrentReads the concurrentReads of this JDBCTap object.
   */
  @Deprecated
  public void setConcurrentReads( int concurrentReads )
    {
    this.concurrentReads = concurrentReads;
    }

  /**
   * Method getPath returns the path of this JDBCTap object.
   *
   * @return the path (type Path) of this JDBCTap object.
   */
  public Path getPath()
    {
    return new Path( getJDBCPath() );
    }

  @Override
  public String getIdentifier()
    {
    return getJDBCPath() + "&id=" + this.id;
    }

  public String getJDBCPath()
    {
    return "jdbc://" + connectionUrl;
    }

  @Deprecated
  public boolean isWriteDirect()
    {
    return true;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Configuration> flowProcess, RecordReader input ) throws IOException
    {
    // input may be null when this method is called on the client side or
    // cluster side when accumulating for a HashJoin
    return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector output ) throws IOException
    {
    if( !isSink() )
      throw new TapException( "this tap may not be used as a sink, no TableDesc defined" );

    return new HadoopTupleEntrySchemeCollector( flowProcess, this, output );
    }

  @Override
  public boolean isSink()
    {
    return tableDesc != null;
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    if( username == null )
      DBConfiguration.configureDB( conf, driverClassName, connectionUrl );
    else
      DBConfiguration.configureDB( conf, driverClassName, connectionUrl, username, password );

    super.sourceConfInit( process, conf );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    if( !isSink() )
      return;

    if( username == null )
      DBConfiguration.configureDB( conf, driverClassName, connectionUrl );
    else
      DBConfiguration.configureDB( conf, driverClassName, connectionUrl, username, password );

    super.sinkConfInit( process, conf );
    }

  private Connection createConnection()
    {
    try
      {
      LOG.info( "creating connection: {}", connectionUrl );
      Class.forName( driverClassName );

      Connection connection = null;

      if( username == null )
        connection = DriverManager.getConnection( connectionUrl );
      else
        connection = DriverManager.getConnection( connectionUrl, username, password );

      connection.setAutoCommit( false );

      return connection;
      }
    catch( SQLException exception )
      {
      if( exception.getMessage().startsWith( "No suitable driver found for" ) )
        {
        List<String> availableDrivers = new ArrayList<String>();
        Enumeration<Driver> drivers = DriverManager.getDrivers();

        while( drivers.hasMoreElements() )
          availableDrivers.add( drivers.nextElement().getClass().getName() );

        LOG.error( "Driver not found: {} because {}. Available drivers are: {}", driverClassName, exception.getMessage(), availableDrivers );
        }
      throw new TapException( exception.getMessage() + " (SQL error code: " + exception.getErrorCode() + ") opening connection: " + connectionUrl, exception );
      }
    catch( Exception exception )
      {
      throw new TapException( "unable to load driver class: " + driverClassName + " because: " + exception.getMessage(), exception);
      }

    }

  /**
   * Method executeUpdate allows for ad-hoc update statements to be sent to the
   * remote RDBMS. The number of rows updated will be returned, if applicable.
   *
   * @param updateString of type String
   * @return int
   */
  public int executeUpdate( String updateString ) throws IOException
    {
    Connection connection = null;
    try
      {
      connection = createConnection();
      return JDBCUtil.executeUpdate( connection, updateString );
      }
    finally
      {
      JDBCUtil.closeConnection( connection );
      }
    }

  /**
   * Method executeQuery allows for ad-hoc queries to be sent to the remote
   * RDBMS. A value of -1 for returnResults will return a List of all results
   * from the query, a value of 0 will return an empty List.
   *
   * @param queryString of type String
   * @param returnResults of type int
   * @return List
   */
  public List<Object[]> executeQuery( String queryString, int returnResults ) throws IOException
    {
    try (Connection connection = createConnection())
      {
      return JDBCUtil.executeQuery( connection, queryString, returnResults );
      }
    catch ( SQLException exception )
      {
      throw new IOException( exception );
      }

    }

  @Override
  public boolean prepareResourceForWrite( Configuration conf ) throws IOException
    {
    if( isReplace() && resourceExists( conf ) )
      deleteResource( conf );

    return createResource( conf );
    }

  @Override
  public boolean createResource( Configuration conf ) throws IOException
    {
    if( resourceExists( conf ) )
      return true;

    Connection connection = null;
    try
      {
      connection = createConnection();
      JDBCUtil.createTableIfNotExists( connection, tableDesc );
      }
    finally
      {
      JDBCUtil.closeConnection( connection );
      }

    return resourceExists( conf );
    }

  @Override
  public boolean deleteResource( Configuration conf ) throws IOException
    {
    if( !isSink() )
      return false;

    if( !resourceExists( conf )  )
      return true;

    Connection connection = null;
    try
      {
      connection = createConnection();
      JDBCUtil.dropTable( connection, tableDesc );
      }
    finally
      {
      JDBCUtil.closeConnection( connection );
      }
    return !resourceExists( conf );
    }

  @Override
  public boolean resourceExists( Configuration conf ) throws IOException
    {
    if( !isSink() )
      return true;

    Connection connection = null;
    try
      {
      connection = createConnection();
      return JDBCUtil.tableExists( connection, tableDesc );
      }
    finally
      {
      JDBCUtil.closeConnection( connection );
      }
    }

  @Override
  public long getModifiedTime( Configuration conf ) throws IOException
    {
    return System.currentTimeMillis();
    }

  @Override
  public String toString()
    {
    return "JDBCTap{" + "connectionUrl='" + connectionUrl + '\'' + ", driverClassName='" + driverClassName + '\'' + ", tableDesc="
        + tableDesc + '}';
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( ! ( object instanceof JDBCTap ) )
      return false;
    if( !super.equals( object ) )
      return false;

    JDBCTap jdbcTap = (JDBCTap) object;

    if( connectionUrl != null ? !connectionUrl.equals( jdbcTap.connectionUrl ) : jdbcTap.connectionUrl != null )
      return false;
    if( driverClassName != null ? !driverClassName.equals( jdbcTap.driverClassName ) : jdbcTap.driverClassName != null )
      return false;
    if( password != null ? !password.equals( jdbcTap.password ) : jdbcTap.password != null )
      return false;
    if( tableDesc != null ? !tableDesc.equals( jdbcTap.tableDesc ) : jdbcTap.tableDesc != null )
      return false;
    if( username != null ? !username.equals( jdbcTap.username ) : jdbcTap.username != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( connectionUrl != null ? connectionUrl.hashCode() : 0 );
    result = 31 * result + ( username != null ? username.hashCode() : 0 );
    result = 31 * result + ( password != null ? password.hashCode() : 0 );
    result = 31 * result + ( driverClassName != null ? driverClassName.hashCode() : 0 );
    result = 31 * result + ( tableDesc != null ? tableDesc.hashCode() : 0 );
    result = 31 * result + batchSize;
    return result;
    }
  }
