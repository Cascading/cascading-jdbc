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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for interacting with databases via JDBC.
 */
public class JDBCUtil
  {
  /**Logger*/
  private static final Logger LOG = LoggerFactory.getLogger( JDBCUtil.class );

  /**
   * Method to check if a table exists in the database of the given Connection object
   * */
  public static boolean tableExists( Connection connection, TableDesc tableDesc ) throws IOException
    {
    ResultSet tables = null;
    try
      {
      DatabaseMetaData dbm = connection.getMetaData();
      tables = dbm.getTables( null, null, tableDesc.getTableName(), null );
      if( tables.next() )
        return true;
      tables.close();
      // try again with upper case for oracle compatibility:
      // see http://stackoverflow.com/questions/2942788/check-if-table-exists
      tables = dbm.getTables( null, null, tableDesc.getTableName().toUpperCase(), null );
      if( tables.next() )
        return true;
      }
    catch( SQLException exception )
      {
      throw new IOException( exception );
      }
    finally
      {
      if( tables != null )
        try
          {
          tables.close();
          }
        catch( SQLException exception )
          {
          throw new IOException( exception );
          }
      }
    return false;
    }

  /**
   * Creates a table from the given table descriptor if it does not exist.
   * */
  public static void createTableIfNotExists( Connection connection, TableDesc tableDesc ) throws IOException
    {
    if( tableExists( connection, tableDesc ) )
      return;

    executeUpdate( connection, tableDesc.getCreateTableStatement() );
    }

  /**
   * Executes the given sql query on the given Connection.
   * */
  public static int executeUpdate( Connection connection, String updateString ) throws IOException
    {
    Statement statement = null;
    int result;
    try
      {
      LOG.info( "executing update: {}", updateString );

      statement = connection.createStatement();
      result = statement.executeUpdate( updateString );

      connection.commit();
      statement.close();
      }
    catch( SQLException exception )
      {
      throw new IOException( "SQL error code: " + exception.getErrorCode() + " executing update statement: " + updateString, exception );
      }

    finally
      {
      try
        {
        if( statement != null )
          statement.close();
        }
      catch( SQLException exception )
        {
        throw new IOException( exception );
        }
      }
    return result;
    }

  /**
   * Drops the table described by the table descriptor if it exists.
   * */
  public static void dropTable( Connection connection, TableDesc tableDesc ) throws IOException
    {
    if( tableExists( connection, tableDesc ) )
      executeUpdate( connection, tableDesc.getTableDropStatement() );
    }

  /**
   * Closes the given database connection.
   * */
  public static void closeConnection( Connection connection ) throws IOException
    {
    if ( connection != null )
      {
      try
        {
        if( connection.isClosed() )
          return;
        connection.commit();
        connection.close();
        }
      catch( SQLException exception )
        {
        throw new IOException( exception );
        }
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
   */   public static List<Object[]> executeQuery( Connection connection, String queryString, int returnResults ) throws IOException
    {
    List<Object[]> result = Collections.emptyList();

    LOG.info( "executing query: {}", queryString );
    try( Statement statement = connection.createStatement() )
      {
      ResultSet resultSet = statement.executeQuery( queryString );

      if( returnResults != 0 )
        result = copyResultSet( resultSet, returnResults );

      connection.commit();
      return result;
      }
    catch( SQLException exception )
      {
      throw new IOException( exception );
      }

    }

  /**
   * Helper method to copy a ResultSet into a List of Object arrays.
   * */
  private static List<Object[]> copyResultSet( ResultSet resultSet, int length ) throws SQLException
    {
    List<Object[]> results = new ArrayList<Object[]>();

    if( length == -1 )
      length = Integer.MAX_VALUE;

    int size = resultSet.getMetaData().getColumnCount();

    int count = 0;
    while( resultSet.next() && count < length )
      {
      count++;
      Object[] row = new Object[ size ];

      for( int i = 0; i < row.length; i++ )
        row[ i ] = resultSet.getObject( i + 1 );

      results.add( row );
      }
    return results;
    }


  }
