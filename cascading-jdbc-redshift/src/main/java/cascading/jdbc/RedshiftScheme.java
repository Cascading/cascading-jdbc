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

import java.lang.String;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.jdbc.db.DBInputFormat;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class and {@link RedshiftTap} manage the ability to read and write data to Amazon's Redshift via EMR.
 * Because Redshift data is loaded into Redshift via S3 but or JDBC but always read via JDBC these classes wrap the
 * pairing of an HFS {@link Tap} and a JDBC {@link Tap} for reading behind one {@link Scheme}
 * object.
 */
public class RedshiftScheme extends JDBCScheme
  {

  public static final String DEFAULT_DELIMITER = ",";
  public static final String DEFAULT_QUOTE = "\"";

  private static final Logger LOG = LoggerFactory.getLogger( RedshiftScheme.class );

  private TextDelimited textDelimited;
  private Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> sinkScheme;
  private RedshiftTableDesc redshiftTableDesc;
  private Map<RedshiftFactory.CopyOption, String> copyOptions = new HashMap<RedshiftFactory.CopyOption, String>();

  /**
   * The primary constructor. Any temporary scratch files will be created with default values for filed delimiters. This
   * will work fine for csv, tab delimited and so on but may lead to errors if there is binary data stored in the files.
   *
   * @param redshiftTableDesc description of the table structure.
   */
  public RedshiftScheme( Fields fields, RedshiftTableDesc redshiftTableDesc )
    {
    this( fields, redshiftTableDesc, DEFAULT_DELIMITER, DEFAULT_QUOTE, null, false );
    }

  /**
   * Use this constructor if you need fine-grained control over the temporary file used to stage data for uploading. You
   * almost certainly don't want to do this unless you know for a fact that your data contains, ex. binary data that might
   * cause issues with default column detection (ex. if you use the \001 character).
   *
   * @param redshiftTableDesc description of the table structure.
   * @param delimiter         single character indicating the separator between fields in a file to load
   * @param quoteCharacter    single character to enclose data within a field in cases where the field contains a delimiter
   * @param copyOptions       custom arguments passed to the COPY command for processing. In most cases, proper cleaning of the data
   *                          before sending it to this Tap is a better alternative.
   */
  public RedshiftScheme( Fields fields, RedshiftTableDesc redshiftTableDesc, String delimiter, String quoteCharacter, Map<RedshiftFactory.CopyOption, String> copyOptions, Boolean tableAlias )
    {
    super( fields, redshiftTableDesc.getColumnNames() );
    super.tableAlias = tableAlias;
    // from the perspective of the JDBC-based parent class flag all fields as JDBC types.
    // for the internally managed S3 sink, use HFS tables (where Date is a String) so that the Tap doesn't
    // write out the integer representation.
    this.redshiftTableDesc = redshiftTableDesc;
    this.textDelimited = new TextDelimited( redshiftTableDesc.getHFSFields(), false, new RedshiftSafeDelimitedParser( delimiter, quoteCharacter ) );
    textDelimited.setSinkFields( getSinkFields() );
    this.sinkScheme = this;
    if( copyOptions != null )
      this.copyOptions.putAll( copyOptions );

    if( !this.copyOptions.containsKey( RedshiftFactory.CopyOption.DELIMITER ) )
      this.copyOptions.put( RedshiftFactory.CopyOption.DELIMITER, DEFAULT_DELIMITER );

    this.copyOptions.put( RedshiftFactory.CopyOption.REMOVEQUOTES, null );
    }

  public RedshiftScheme( String[] columns, String[] orderBy, String[] updateBy )
    {
    super( columns, orderBy, updateBy );
    }

  public RedshiftScheme( Class<? extends DBInputFormat> inputFormat, Fields fields, String[] columns )
    {
    super( inputFormat, fields, columns );
    }

  public RedshiftScheme( String[] columnsNames, String contentsQuery, String countStarQuery )
    {
    super( columnsNames, contentsQuery, countStarQuery );
    }

  public TextDelimited getTextDelimited()
    {
    return textDelimited;
    }

  public TableDesc getRedshiftTableDesc()
    {
    return redshiftTableDesc;
    }

  public Map<RedshiftFactory.CopyOption, String> getCopyOptions()
    {
    return copyOptions;
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf )
    {
    if( ( (RedshiftTap) tap ).isUseDirectInsert() )
      {
      sinkScheme = this;
      super.sinkConfInit( flowProcess, tap, jobConf );
      }
    else
      {
      sinkScheme = textDelimited;
      sinkScheme.sinkConfInit( flowProcess, tap, jobConf );
      }
    }

  @Override
  public String toString()
    {
    if( getSinkFields().equals( getSourceFields() ) )
      return getClass().getSimpleName() + "[" + getSourceFields().print() + "]";
    else
      return getClass().getSimpleName() + "[" + getSourceFields().print() + "->" + getSinkFields().print() + "]";
    }


  }
