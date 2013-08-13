package cascading.provider.jdbc;

import static org.junit.Assert.fail;

import org.junit.Before;

public class GenericJdbcCheck extends JDBCTestingBase
  {

  public final static String JDBC_URL_PROPERTY_NAME = "cascading.jdbcurl";

  public final static String JDBC_DRIVER_PROPERTY_NAME = "cascading.jdbcdriver";

  @Before
  public void setUp()
    {
    if ( System.getProperty( JDBC_DRIVER_PROPERTY_NAME ) == null || System.getProperty( JDBC_URL_PROPERTY_NAME ) == null )
      fail( String.format( "please set the '%s' and '%s' system properties", 
                JDBC_DRIVER_PROPERTY_NAME, JDBC_URL_PROPERTY_NAME ) );

    setJdbcurl( System.getProperty( JDBC_URL_PROPERTY_NAME ) );
    setDriverName( System.getProperty( JDBC_DRIVER_PROPERTY_NAME ) );
    }
  }
