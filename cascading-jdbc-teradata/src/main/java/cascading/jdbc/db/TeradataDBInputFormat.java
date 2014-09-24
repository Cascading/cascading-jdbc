/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.jdbc.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import cascading.CascadingException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Extends DBInputFormat
 * Modifies SELECT query
 */
@SuppressWarnings("rawtypes")
public class TeradataDBInputFormat extends DBInputFormat<DBWritable>
  {
  /**
   * Currently used for testing
   */
  @Override
  protected void setAutoCommit( Connection connection )
    {
    try
      {
      connection.setAutoCommit( true );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to set auto commit", exception );
      }
    }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RecordReader<LongWritable, DBWritable> getRecordReaderInternal( cascading.jdbc.db.DBInputFormat.DBInputSplit split, Class inputClass, JobConf job ) throws SQLException, IOException
    {
    return new TeradataDBRecordReader( split, inputClass, job );
    }

  class TeradataDBRecordReader extends DBInputFormat.DBRecordReader
    {
    protected TeradataDBRecordReader( cascading.jdbc.db.DBInputFormat.DBInputSplit split, Class inputClass, JobConf job ) throws SQLException, IOException
      {
      super( new cascading.jdbc.db.DBInputFormat.DBInputSplit(), inputClass, job );
      }

    /** Returns the query for selecting the records from an Teradata DB.
     * omits the LIMIT and OFFSET for FASTEXPORT
     */
    /**
     * {@inheritDoc}
     */
    public String getSelectQuery()
      {
      StringBuilder query = new StringBuilder();

      if( dbConf.getInputQuery() == null )
        {
        query.append( "SELECT " );

        for( int i = 0; i < fieldNames.length; i++ )
          {
          query.append( fieldNames[ i ] );

          if( i != fieldNames.length - 1 )
            query.append( ", " );
          }

        query.append( " FROM " ).append( tableName );

        if( conditions != null && conditions.length() > 0 )
          query.append( " WHERE (" ).append( conditions ).append( ")" );

        String orderBy = dbConf.getInputOrderBy();

        if( orderBy != null && orderBy.length() > 0 )
          query.append( " ORDER BY " ).append( orderBy );
        }
      else
        query.append( dbConf.getInputQuery() );

      return query.toString();
      }
    }
  }





