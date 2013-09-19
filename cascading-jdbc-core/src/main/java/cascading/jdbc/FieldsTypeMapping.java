/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import com.google.common.collect.Maps;


/**
 * 
 * Class that maps java classes to SQL type definitions.
 * 
 */
public class FieldsTypeMapping
  {
  private static final Map<Class<?>, String> TYPES = Maps.newHashMap();

  static
    {
    TYPES.put( Integer.class, "int" );
    TYPES.put( int.class, "int not null" );
    TYPES.put( String.class, "varchar(256)" );
    TYPES.put( Long.class, "int" );
    TYPES.put( long.class, "int not null" );
    TYPES.put( Date.class, "date" );
    TYPES.put( Timestamp.class, "timestamp" );
    }

  
  /**
   * Returns a mapping of a java class to a SQL type. 
   * 
   * @param klass The class to find the mapping for.
   * 
   * @throws IllegalArgumentException If no mapping can  be found.
   * */
  public static String sqltypeForClass(Class<?> klass)
    {
    String sqlType = TYPES.get( klass );
    if (sqlType == null)
      throw new IllegalArgumentException(String.format("cannot map java class %s to a sql type", klass));
    return sqlType;
    }
  }
