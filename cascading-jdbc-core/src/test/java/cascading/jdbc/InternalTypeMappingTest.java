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

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.Test;

import cascading.lingual.type.SQLDateCoercibleType;
import cascading.lingual.type.SQLTimeCoercibleType;
import cascading.lingual.type.SQLTimestampCoercibleType;

public class InternalTypeMappingTest
  {

  @Test
  public void testMappings()
    {
    assertEquals( "int not null",  InternalTypeMapping.sqltypeForClass( int.class ) );
    assertEquals( "int not null", InternalTypeMapping.sqltypeForClass( long.class ) );
    assertEquals( "int", InternalTypeMapping.sqltypeForClass( Integer.class ) );
    assertEquals( "int", InternalTypeMapping.sqltypeForClass( Long.class ) );
    assertEquals( "varchar(256)", InternalTypeMapping.sqltypeForClass( String.class ) );
    assertEquals( "timestamp", InternalTypeMapping.sqltypeForClass( Timestamp.class ) );
    assertEquals( "time", InternalTypeMapping.sqltypeForClass( Time.class ) );
    assertEquals( "date", InternalTypeMapping.sqltypeForClass( Date.class ) );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownClass()
    {
    InternalTypeMapping.sqltypeForClass( boolean.class );
    }

  @Test
  public void testTypeWithCoercibles()
    {
    assertEquals( "date", InternalTypeMapping.sqltypeForClass( new SQLDateCoercibleType() ) );
    assertEquals( "time", InternalTypeMapping.sqltypeForClass( new SQLTimeCoercibleType() ) );
    assertEquals( "timestamp", InternalTypeMapping.sqltypeForClass( new SQLTimestampCoercibleType() ) );
    }
  }