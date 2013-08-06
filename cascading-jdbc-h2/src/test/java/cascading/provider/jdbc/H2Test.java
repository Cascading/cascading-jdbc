package cascading.provider.jdbc;

import org.junit.Before;


/**
 * Runs the tests against an instance of h2: http://www.h2database.com/html/main.html
 * */
public class H2Test extends JDBCTestingBase {

    @Before
	public void setUp()
      { 
		setDriverName("org.h2.Driver");
		setJdbcurl("jdbc:h2:mem:testing;DB_CLOSE_DELAY=-1;MVCC=true");
      }
}
