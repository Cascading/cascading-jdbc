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

package redshift;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.jdbc.AWSCredentials;
import cascading.jdbc.RedshiftScheme;
import cascading.jdbc.RedshiftTableDesc;
import cascading.jdbc.RedshiftTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SampleFlow
  {

  private static final Logger LOG = LoggerFactory.getLogger( SampleFlow.class );

  public static void main( String[] args )
    {

    String inputFilesPath = args[ 0 ];
    String tempPath = args[ 1 ];
    String redshiftJdbcUrl = args[ 2 ];
    String redshiftUsername = args[ 3 ];
    String redshiftPassword = args[ 4 ];
    String accessKey = args[ 5 ];
    String secretKey = args[ 6 ];

    Properties properties = new Properties();
    properties.setProperty( "fs.s3n.awsAccessKeyId", accessKey );
    properties.setProperty( "fs.s3n.awsSecretAccessKey", secretKey );
    AppProps.setApplicationJarClass( properties, SampleFlow.class );

    String[] fieldNames = {"ID", "SPECIES", "LOCATION", "NAME", "TIMESEEN"};
    String[] fieldTypes = {"SMALLINT", "VARCHAR(30)", "VARCHAR(30)", "VARCHAR(30)", "date"};
    String distributionKey = "ID";
    String[] sortKeys = {"SPECIES", "LOCATION"};
    Fields sampleFields = new Fields( fieldNames );

    Pipe debugPipe = new Pipe( "debug-insert" );
    debugPipe = new Each( debugPipe, DebugLevel.VERBOSE, new Debug( true ) );

    //
    // Example of writing data to Redshift from S3-backed HFS.
    //

    String targetRedshiftTable = "results";
    Tap inputFilesTap = new Hfs( new TextDelimited( sampleFields, false, ",", "\"" ), inputFilesPath );
    RedshiftTableDesc redshiftTableDesc = new RedshiftTableDesc( targetRedshiftTable, fieldNames, fieldTypes, distributionKey, sortKeys );
    RedshiftScheme redshiftScheme = new RedshiftScheme( sampleFields, redshiftTableDesc );
    AWSCredentials awsCredentials = new AWSCredentials( accessKey, secretKey );
    Tap outputTableTap = new RedshiftTap( redshiftJdbcUrl, redshiftUsername, redshiftPassword, tempPath, awsCredentials, redshiftTableDesc, redshiftScheme, SinkMode.REPLACE, true, false );

    LOG.info( "-------------------------------------------------------------" );
    LOG.info( " writing data to table: {} in database: {}", redshiftTableDesc.getTableName(), redshiftJdbcUrl );
    LOG.info( "-------------------------------------------------------------" );

    FlowDef flowDef = FlowDef.flowDef();
    flowDef.setName( "redshift-flow-insert" );
    flowDef.addSource( debugPipe, inputFilesTap );
    flowDef.addTailSink( debugPipe, outputTableTap );
    flowDef.setDebugLevel( DebugLevel.VERBOSE );

    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
    Flow redshiftFlow = flowConnector.connect( flowDef );
    redshiftFlow.complete();

    LOG.info( "-------------------------------------------------------------" );
    LOG.info( "completed writing data" );
    LOG.info( "-------------------------------------------------------------" );

    //
    // Example of copying data from one Redshift table to another
    //
    Tap redshiftInputTap = new RedshiftTap( redshiftJdbcUrl, redshiftUsername, redshiftPassword, tempPath, awsCredentials, redshiftTableDesc, redshiftScheme, SinkMode.KEEP );
    RedshiftTableDesc redshiftDestTableDesc = new RedshiftTableDesc( "results2", fieldNames, fieldTypes, distributionKey, sortKeys );
    RedshiftScheme redshiftDestScheme = new RedshiftScheme( sampleFields, redshiftDestTableDesc );
    Tap outputDestTableTap = new RedshiftTap( redshiftJdbcUrl, redshiftUsername, redshiftPassword, tempPath, awsCredentials, redshiftDestTableDesc, redshiftDestScheme, SinkMode.REPLACE, true, false );

    LOG.info( "-------------------------------------------------------------" );
    LOG.info( "copying data to DB table: {} in database: {}", redshiftDestTableDesc.getTableName(), redshiftJdbcUrl );
    LOG.info( "-------------------------------------------------------------" );

    FlowDef flowDefCopy = FlowDef.flowDef();
    flowDefCopy.setName( "redshift-flow-copy" );
    flowDefCopy.addSource( debugPipe, redshiftInputTap );
    flowDefCopy.addTailSink( debugPipe, outputDestTableTap );
    flowDefCopy.setDebugLevel( DebugLevel.VERBOSE );

    HadoopFlowConnector copyFlowConnector = new HadoopFlowConnector( properties );
    Flow redshiftCopyFlow = copyFlowConnector.connect( flowDefCopy );
    redshiftCopyFlow.complete();

    LOG.info( "-------------------------------------------------------------" );
    LOG.info( "copy data to DB table complete", redshiftDestTableDesc.getTableName() );
    LOG.info( "-------------------------------------------------------------" );

    //
    // Example of exporting data from one Redshift table to S3
    //

    Tap exportFilesTap = new Hfs( new TextDelimited( sampleFields, false, "\t", "\'" ), inputFilesPath + ".out", SinkMode.REPLACE );

    LOG.info( "-------------------------------------------------------------" );
    LOG.info( "copying data to S3 bucket: {}", exportFilesTap.getIdentifier() );
    LOG.info( "-------------------------------------------------------------" );

    FlowDef flowDefExport = FlowDef.flowDef();
    flowDefExport.setName( "redshift-flow-export" );
    flowDefExport.addSource( debugPipe, redshiftInputTap );
    flowDefExport.addTailSink( debugPipe, exportFilesTap );
    flowDefExport.setDebugLevel( DebugLevel.VERBOSE );

    HadoopFlowConnector exportFlowConnector = new HadoopFlowConnector( properties );
    Flow exportFlow = exportFlowConnector.connect( flowDefExport );
    exportFlow.complete();
    LOG.info( "-------------------------------------------------------------" );
    LOG.info( "copy data to S3 bucket complete" );
    LOG.info( "-------------------------------------------------------------" );

    }

  }
