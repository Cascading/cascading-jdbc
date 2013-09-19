package cascading.provider.jdbc;

import org.junit.Before;

/**
 * Runs the tests against an instance of h2:
 * http://www.h2database.com/html/main.html
 * */
public class PostgresTest extends JDBCTestingBase
  {

  @Before
  public void setUp()
    {
    setDriverName( "org.postgresql.Driver" );
    setJdbcurl( System.getProperty( "cascading.jdbcurl" ) );
    }
  }
