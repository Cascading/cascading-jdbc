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

import cascading.scheme.util.DelimitedParser;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import org.apache.hadoop.util.StringUtils;

/** {@link DelimitedParser} that treats the presence of characters that Redshift can't handle as an error in that line. */

public class RedshiftSafeDelimitedParser extends DelimitedParser
  {

  private static final char BACKSLASH = 0x5c;

  public RedshiftSafeDelimitedParser( String delimiter, String quote, Class[] types, boolean strict, boolean safe, Fields sourceFields, Fields sinkFields )
    {
    super( delimiter, quote, types, strict, safe, sourceFields, sinkFields );
    }

  public RedshiftSafeDelimitedParser( String delimiter, String quote )
    {
    this( delimiter, quote, null, true, true, null, null );
    }

  @Override
  public Appendable joinLine( Iterable iterable, Appendable buffer )
    {
    try
      {
      return joinWithQuote( iterable, buffer );
      }
    catch( IOException e )
      {
      throw new TapException( "unable to append data", e );
      }
    }

  protected Appendable joinWithQuote( Iterable tuple, Appendable buffer ) throws IOException
    {
    int count = 0;

    for( Object value : tuple )
      {
      if( count != 0 )
        buffer.append( delimiter );

      if( value != null )
        {
        if( value instanceof String )
          {
          String valueString = value.toString();

          if( containsAnyInvalidCodepoints( valueString ) )
            {
            throw new InvalidCodepointForRedshiftException( valueString );
            }

          String escaped = StringUtils.escapeString( valueString, BACKSLASH, new char[]{'"', '\''} );
          buffer.append( quote ).append( escaped ).append( quote );
          }
        else
          {
          buffer.append( value.toString() );
          }
        }

      count++;
      }

    return buffer;
    }

  private boolean containsAnyInvalidCodepoints( String s )
    {
    for( int i = 0; i < s.length(); i++ )
      {
      if( isExcludedCodepoint( s.codePointAt( i ) ) )
        {
        return true;
        }
      }
    return false;
    }

  private boolean isExcludedCodepoint( int codepoint )
    {
    if( codepoint >= 0xD800 && codepoint <= 0xDFFF )
      {
      return true;
      }
    if( codepoint >= 0xFDD0 && codepoint <= 0xFDEF )
      {
      return true;
      }
    if( codepoint >= 0xFFFE && codepoint <= 0xFFFF )
      {
      return true;
      }
    return false;
    }
  }
