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

import org.apache.hadoop.conf.Configuration;

public class MySqlDBConfiguration
  {

  /** Boolean to use ON DUPLICATE KEY UPDATE for INSERTs when outputting tuples to MySQL. */
  public static final String REPLACE_ON_INSERT = "mapred.jdbc.output.replace.on.insert";

  private Configuration job;

  public MySqlDBConfiguration( Configuration job )
    {
    this.job = job;
    }

  public boolean getReplaceOnInsert()
    {
    return job.getBoolean( MySqlDBConfiguration.REPLACE_ON_INSERT, false );
    }

  public void setReplaceOnInsert( boolean replaceOnInsert )
    {
    job.setBoolean( MySqlDBConfiguration.REPLACE_ON_INSERT, replaceOnInsert );
    }

  }
