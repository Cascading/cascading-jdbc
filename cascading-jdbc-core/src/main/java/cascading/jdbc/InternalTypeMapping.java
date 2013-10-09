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

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;

import com.google.common.collect.Maps;

/**
 * Class that maps Java classes to SQL type definitions. When cascading-jdbc is
 * used from lingual, we might be presented with JVM internal classes and
 * {@link CoercibleType} instances of {@link Fields}, but we need to know the
 * more specific JDBC types, in order to write the tuple to the databases. This
 * class is doing an "educated guess" on the types. 
 * 
 * The rules are like this:
 * <pre>
 * int.class -> "int not null"
 * Integer.class -> "int"
 * String.class -> "varchar(256)"
 * long.class -> "int not null"
 * Long.class -> "int"
 * Time.class -> "time"
 * Date.class -> "date"
 * Timestamp.class -> "timestamp".
 * </pre>
 * 
 * 
 * Coercibles are translated like this and then given to the translation above:
 * 
 * <pre>
 * cascading.lingual.type.SQLDateCoercibleType -> java.sql.Date.class
 * cascading.lingual.type.SQLDateTimeCoercibleType -> java.sql.Date.class
 * cascading.lingual.type.SQLTimeCoercibleType -> java.sql.Time.class
 * cascading.lingual.type.SQLTimestampCoercibleType ->  java.sql.Timestamp.class
 * </pre>
 * 
 * All other {@link CoercibleType} instances are converted to String.class, since anything should
 * be coercible to a String.
 * 
 */
public class InternalTypeMapping
  {
  private static final Map<Type, String> TYPES = Maps.newHashMap();

  private static final Map<String, Type> NATIVE_TYPES = Maps.newHashMap();

  static
    {
    TYPES.put( Integer.class, "int" );
    TYPES.put( int.class, "int not null" );
    TYPES.put( String.class, "varchar(256)" );
    TYPES.put( Long.class, "int" );
    TYPES.put( long.class, "int not null" );
    TYPES.put( Time.class, "time" );
    TYPES.put( Date.class, "date" );
    TYPES.put( Timestamp.class, "timestamp" );

    /*
     * we have no compile time dependency on lingual and we should never have
     * that, so we work around the types being unknown right now, by using class names.
     */
    NATIVE_TYPES.put( "cascading.lingual.type.SQLDateCoercibleType", java.sql.Date.class );
    NATIVE_TYPES.put( "cascading.lingual.type.SQLDateTimeCoercibleType", java.sql.Date.class );
    NATIVE_TYPES.put( "cascading.lingual.type.SQLTimeCoercibleType", java.sql.Time.class );
    NATIVE_TYPES.put( "cascading.lingual.type.SQLTimestampCoercibleType", java.sql.Timestamp.class );

    }

  /**
   * Method to determine the correct type, that a field should be
   * coerced to, before writing it to the database. The method uses an internal
   * mapping. If no class can be found in the mapping, it will return
   * <code>String.class</code>;
   * 
   * @param type The type of a {@link Fields} instance
   * @return a JVM internal type.
   */
  public static Type findInternalType( Type type )
    {
    if( ! ( type instanceof CoercibleType ) )
      return type;

    CoercibleType<?> coercible = (CoercibleType<?>) type;
    Type nativeType = NATIVE_TYPES.get( coercible.getClass().getName() );
    if( nativeType == null )
      nativeType = String.class;
    return nativeType;

    }

  /**
   * Returns a mapping of a java class to a SQL type as a {@link String}.
   * 
   * @param type The {@link Type} to find the mapping for.
   * 
   * @throws IllegalArgumentException If no mapping can be found.
   * */
  public static String sqltypeForClass( Type type )
    {
    String sqlType = TYPES.get( type );
    if( sqlType == null )
      {
      Type nativeType = findInternalType( type );
      sqlType = TYPES.get( nativeType );
      if( sqlType == null )
        throw new IllegalArgumentException( String.format( "cannot map type %s to a sql type", type ) );
      }
    return sqlType;
    }
  }
