package cascading.provider.jdbc;

import org.junit.Before;

/**
 * Runs the tests against an instance of h2:
 * http://www.h2database.com/html/main.html
 * */
public class MysqlTest extends JDBCTestingBase
  {

  @Before
  public void setUp()
    {
    setDriverName( "com.mysql.jdbc.Driver" );
    setJdbcurl( System.getProperty( "cascading.jdbcurl" ) );
    }
  }
