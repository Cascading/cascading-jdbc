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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MySqlDBInputFormat extends DBInputFormat<DBWritable>
  {

  protected class MySqlDBRecordReader extends DBRecordReader
    {
    protected MySqlDBRecordReader( DBInputSplit split, Class<DBWritable> inputClass, JobConf job ) throws SQLException, IOException
      {
      super( split, inputClass, job );
      }

    @Override
    protected Statement createStatement() throws SQLException
      {
      Statement statement = connection.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
      statement.setFetchSize( Integer.MIN_VALUE );
      return statement;
      }
    }

  @Override
  protected RecordReader<LongWritable, DBWritable> getRecordReaderInternal( DBInputSplit split, Class inputClass, JobConf job ) throws SQLException,
    IOException
    {
    return new MySqlDBRecordReader( split, inputClass, job );
    }

  }
