/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.sql.SQLException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.jdbc.JDBCFactory;

@SuppressWarnings("rawtypes")
public class OracleDBInputFormat extends DBInputFormat<DBWritable>
  {
    class OracleDBRecordReader extends DBInputFormat.DBRecordReader
    {

      protected OracleDBRecordReader( cascading.jdbc.db.DBInputFormat.DBInputSplit split, Class inputClass,
          JobConf job ) throws SQLException, IOException
        {
        super( split, inputClass, job );
        }
      
      /** Returns the query for selecting the records from an Oracle DB. */
      protected String getSelectQuery() {
        StringBuilder query = new StringBuilder();

        // Oracle-specific codepath to use rownum instead of LIMIT/OFFSET.
        if(dbConf.getInputQuery() == null) {
          query.append("SELECT ");
      
          for (int i = 0; i < fieldNames.length; i++) {
            query.append(fieldNames[i]);
            if (i != fieldNames.length -1) {
              query.append(", ");
            }   
          }   
      
          query.append(" FROM ").append(tableName);
          if (conditions != null && conditions.length() > 0)
            query.append(" WHERE ").append(conditions);
          String orderBy = dbConf.getInputOrderBy();
          if (orderBy != null && orderBy.length() > 0) {
            query.append(" ORDER BY ").append(orderBy);
          }   
        } else {
          //PREBUILT QUERY
          query.append(dbConf.getInputQuery());
        }   
        
        try {
          
          if (split.getLength() > 0 && split.getStart() >= 0){ 
            String querystring = query.toString();

            query = new StringBuilder();
            query.append("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( ");
            query.append(querystring);
            query.append(" ) a WHERE rownum <= ").append(split.getStart());
            query.append(" + ").append(split.getLength());
            query.append(" ) WHERE dbif_rno >= ").append(split.getStart() + 1 );
          }   
        } catch (IOException ex) {
          // ignore, will not throw.
        }    

        return query.toString();
      }

    }
    
    @Override
    protected RecordReader<LongWritable, DBWritable> getRecordReaderInternal( cascading.jdbc.db.DBInputFormat.DBInputSplit split,
      Class inputClass, JobConf job ) throws SQLException, IOException
    {
    return new OracleDBRecordReader( split, inputClass, job );
    }
  }
