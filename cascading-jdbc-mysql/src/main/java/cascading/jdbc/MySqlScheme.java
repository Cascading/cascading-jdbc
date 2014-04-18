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

package cascading.jdbc;

public class MySqlScheme extends JDBCScheme
  {

  /**
   * If true, will use mysql's 'ON DUPLICATE KEY UPDATE' to update existing rows with the same key
   * with the new data. See http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html.
   */
  private boolean replaceOnInsert = false;

  /**
   * Constructor MySqlScheme creates a new MySqlScheme instance.
   *
   * Specify replaceOnInsert if you want to change the default insert behavior.
   *
   * @param inputFormatClass of type Class<? extends DBInputFormat>
   * @param columns of type String[]
   * @param orderBy of type String[]
   * @param conditions of type String
   * @param updateBy of type String[]
   * @param replaceOnInsert of type boolean
   */
  public JDBCScheme( Class<? extends DBInputFormat> inputFormatClass, String[] columns, String[] orderBy,
      String conditions, String[] updateBy, boolean replaceOnInsert )
    {
    super( inputFormatClass, MySqlDBOutputFormat.class, columns, orderBy, conditions, -1, updateBy );
    this.replaceOnInsert = replaceOnInsert;
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf job )
    {
    MySqlDBConfiguration conf = new MySqlDBConfiguration( job );
    conf.setReplaceOnInsert( replaceOnInsert );

    super.sinkConfInit( process, tap, job );
    }
  }
