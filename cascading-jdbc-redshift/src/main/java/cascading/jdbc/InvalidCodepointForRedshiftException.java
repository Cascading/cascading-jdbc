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

/** Indicates that a line had a codepoint that */


public class InvalidCodepointForRedshiftException extends RuntimeException
  {

  private final String originalString;

  public InvalidCodepointForRedshiftException( String originalString )
    {
    this.originalString = originalString;
    }

  @Override
  public String getMessage()
    {
    return String.format( "The string contains characters not allowed in a Redshift DB. Original string: \"%s\"", originalString );
    }
  }
