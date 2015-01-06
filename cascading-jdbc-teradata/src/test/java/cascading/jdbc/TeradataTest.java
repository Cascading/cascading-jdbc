/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.jdbc.TeradataTableDesc;
import cascading.jdbc.db.TeradataDBInputFormat;
import org.junit.Before;

public class TeradataTest extends JDBCTestingBase
  {
  @Before
  public void setUp()
    {
    setDriverName( "com.teradata.jdbc.TeraDriver" );
    setJdbcurl( System.getProperty( "cascading.jdbcurl" ) );
    setInputFormatClass( TeradataDBInputFormat.class );
    setFactory( new TeradataJDBCFactory() );
    }

  @Override
  public TeradataTableDesc getNewTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys )
    {
    return new TeradataTableDesc( tableName, columnNames, columnDefs, primaryKeys );
    }
  }