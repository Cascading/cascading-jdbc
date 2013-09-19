package cascading.jdbc;

import org.junit.Before;

public class OracleTest extends JDBCTestingBase
  {

  @Before
  public void setUp()
    {
    setDriverName( "oracle.jdbc.OracleDriver" );
    setJdbcurl( System.getProperty( "cascading.jdbcurl" ) );
    }
  }
