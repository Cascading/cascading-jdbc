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

package cascading.jdbc.db;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class MySqlDBOutputFormat<K extends DBWritable, V> extends DBOutputFormat<K, V>
  {

  private boolean replaceOnInsert = false;

  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter( FileSystem filesystem, JobConf job, String name, Progressable progress ) throws IOException
    {
    MySqlDBConfiguration dbConf = new MySqlDBConfiguration( job );
    replaceOnInsert = dbConf.getReplaceOnInsert();

    return super.getRecordWriter( filesystem, job, name, progress );
    }

  /** {@inheritDoc} */
  @Override
  protected String constructInsertQuery( String table, String[] fieldNames )
    {
    StringBuilder query = new StringBuilder( super.constructInsertQuery( table, fieldNames ) );
    if( replaceOnInsert )
      {
      query.append( " ON DUPLICATE KEY UPDATE " );
      for( int i = 0; i < fieldNames.length; i++ )
        {
        query.append( String.format( "%s=VALUES(%s)", fieldNames[i], fieldNames[i] ) );
        if( i != fieldNames.length - 1 )
          {
          query.append( "," ); }
          }
      }
    return query.toString();
    }
  }
