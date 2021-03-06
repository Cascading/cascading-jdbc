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

import java.io.Serializable;

/** Holder for the AWS credentials. {@link Serializable} is required for EMR use. */
public class AWSCredentials implements Serializable
  {
  private String awsAccessKey;
  private String awsSecretKey;

  public final static AWSCredentials RUNTIME_DETERMINED = new AWSCredentials( AWSCredentials.class.getName(), AWSCredentials.class.getName() );

  public AWSCredentials( String awsAccessKey, String awsSecretKey )
    {
    this.awsAccessKey = awsAccessKey;
    this.awsSecretKey = awsSecretKey;
    }

  public String getAwsAccessKey()
    {
    return awsAccessKey;
    }

  public String getAwsSecretKey()
    {
    return awsSecretKey;
    }

  public boolean isBlank() {
    return awsAccessKey == null && awsSecretKey == null;
  }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof AWSCredentials ) )
      return false;

    AWSCredentials that = (AWSCredentials) object;

    if( awsAccessKey != null ? !awsAccessKey.equals( that.awsAccessKey ) : that.awsAccessKey != null )
      return false;
    if( awsSecretKey != null ? !awsSecretKey.equals( that.awsSecretKey ) : that.awsSecretKey != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = awsAccessKey != null ? awsAccessKey.hashCode() : 0;
    result = 31 * result + ( awsSecretKey != null ? awsSecretKey.hashCode() : 0 );
    return result;
    }

  }
