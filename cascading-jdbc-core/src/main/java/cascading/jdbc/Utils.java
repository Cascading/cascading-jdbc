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

import com.google.common.base.Strings;

/**
 *
 */
public class Utils
  {

  /**
   * Determines if a given string is <code>null</code> empty or only consists of
   * whitespace characters.
   *
   * @param string The string to check.
   * @return <code>true</code> if any of the above is true, otherwise
   * <code>false</code>.
   */
  public static boolean isNullOrEmpty( String string )
    {
    return string == null || Strings.isNullOrEmpty( string.trim() );
    }

  /**
   * Checks if the given String is null, empty or only contains whitespace
   * characeters and if that is a case, throws an IllegalArgumentException.
   *
   * @param string           The string to check.
   * @param exceptionMessage The message to be used in the Exception.
   * @throws IllegalArgumentException.
   */
  public static String throwIfNullOrEmpty( String string, String exceptionMessage )
    {
    if( isNullOrEmpty( string ) )
      throw new IllegalArgumentException( exceptionMessage );
    return string;
    }

  }
