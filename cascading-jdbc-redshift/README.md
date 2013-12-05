# cascading.redshift

This code provides support for Amazon's [Redshift](http://aws.amazon.com/redshift/) for [Cascading](http://cascading.org) and [Lingual](http://cascading.org/lingual). This allows users
to treat Redshift as a Cascading Tap or register Redshift as a Provider in Lingual.

This can be used in an EMR-based workflow, as part of a hadoop-based Cascading Tap, in Lingual's shell mode for standalone SQL queries, or as part of Lingual's JDBC mechanism to integrate
relational databases with Hadoop flows.

See the specific projects for details and see [cascding-jdbc](https://github.com/Cascading/cascading-jdbc) for more examples of JDBC connectors.

## Quick Start for Advanced Users

Users who are already familiar with Redshift, Cascading and Lingual can make use of this by adding the compiled library to their existing projects.

[cascading-redshift](http://conjars.org/cascading/cascading-redshift) is hosted on [conjars.org](http://conjars.org) and can be included in an existing Maven or Gradle project by
adding the conjars repo `http://conjars.org/repo/` to your repo list and then adding either

Maven:
```xml
<dependency>
  <groupId>cascading</groupId>
  <artifactId>cascading-redshift</artifactId>
  <version>2.2</version>
</dependency>
```
Gradle:

`compile group: 'cascading', name: 'cascading-redshift', version: '0.16'`

The source code in the following files should be sufficient for Cascading and Lingual users who want to know cascading-redshift specific features. All other users should
see the steps in the more detailed Example Code sections below.

Running as a Cascading Flow:
`src/main/java/redshift/SampleFlow.java`

Running in EMR:
`src/main/resources/emrExample.sh`

Running as a Lingual provider:
`src/main/resources/lingualShellExample.sh`

## Example Code and Detailed Installation

To run the sample code, clone this repo and compile the code with the command:
`gradle sampleCode`

This will build all the components needed for the examples. The compile has been tested against [Gradle 1.9 and 1.6](http://www.gradle.org/) but since it doesn't make use of any
complex Gradle tasks beyond the basic compile it is likely to compile properly with any Gradle version. The code supports JDK 1.6.

### Set up AWS

Redshift is an AWS-specific tool and hence all the example code makes use of AWS. This tutorial does not cover starting up a Redshift Database, AWS permission rules, and general EC2 management.
See the [Redshift Documentation](http://aws.amazon.com/redshift/) for details on how to set that up. In particular, if you are using EMR to run the flow your EMR instances will need to be in a
security group that has access to the database and it is strongly suggested that you run your EMR instances in the same availability zone your Redshift database is running in.

Before proceding with the tutorial you should verify your Redshift setup is complete and accessible using the "psql" tool to log in to Redshift and should run a basic EMR job to confirm that your
EMR setup is valid.

### Set AWS Credentials

Since Redshift reads the data initially from S3, you have to provide a valid aws
access-key/secret-key combination. There are multiple options to do that:

- Put them in the redshift-protocol.properties file as `awsAccessKey` and `awsSecretKey`
- Set them as the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`
- Put them in the your `mapred-site.xml` file as `fs.s3n.awsAccessKeyId` and
  `fs.s3n.awsSecretAccessKey`

If you are running your jobs on Amazon EMR, the credentials will be in the
job-conf and will automatically be picked up from there.

### Example: Running Redshift in Cascading

An example with Cascading to sink data to Redshift, coping data from one Redshift table to another, and extracting data from Redshift to S3-backed HFS.
Before running this code, make sure that you have set up AWS and that you have installed the [EMR Command Line Tools](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-install.html).

The file SampleFlow.java contains three flows to demonstrate integration with Cascading and Cascading's HFS Tap.
You can compile the code and launch an EMR instance by running:
`./src/main/resources/emrExample.sh [JDBC URL] [Redshift DB user] [Redshift DB password] [S3 bucket to read and write data in] [AWS availability zone]`

If the task completes succesfully, you will have two tables in S3 "results" and "results2" a file in your S3 bucket name sampleData.csv and a directory in your S3 bucket named sampleData.csv.out containing
the part-XXXX files from the M/R job that extracted and transformed the DB data.

### Example: Running Redshift as a Lingual Provider

The Redshift code can also be used as a Lingual provider. This requires a install of  [Lingual
](http://docs.cascading.org/lingual/1.0/), the AWS tools described above, and a run of the Cascading Sample
to populate the database with some information.

Running the sample assumes that you do not have an existing Lingual catalog that you need to preserve. If you do, back up your catalog before running it
since this sample will re-initialize the catalog from scratch.

To compile and run the lingual example execute.
`/src/main/resources/lingualShellExample.sh [JDBC URL] [Redshift DB user] [Redshift DB password]`

This script registers the provider and issues a "SELECT * FROM results" query to the command line.



