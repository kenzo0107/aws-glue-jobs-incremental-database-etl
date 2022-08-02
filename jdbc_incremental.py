#  Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Amazon Software License (the "License"). You may not use
#  this file except in compliance with the License. A copy of the License is
#  located at:
#
#    http://aws.amazon.com/asl/
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.

import json
import hashlib
from datetime import datetime

from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

import utils


class Resource(object):
    def __init__(self, database, table_name, kind, catalog_id=None):
        self.database = database
        self.table_name = table_name
        self.catalog_id = catalog_id
        self.kind = kind


class Source(Resource):
    def __init__(self, database, table_name, source_kind, catalog_id):
        Resource.__init__(self, database, table_name, "SOURCE", catalog_id)
        self.source_kind = source_kind


class JDBCSource(Source):
    def __init__(
        self, database, table_name, catalog_id, source_prefix, partition_spec=""
    ):
        Source.__init__(self, database, table_name, "JDBC", catalog_id)
        self.source_prefix = source_prefix
        self.partition_spec = partition_spec.split("/") if partition_spec else []


class Target(Resource):
    def __init__(
        self,
        database,
        table_name,
        catalog_id,
        S3_location,
        target_prefix,
        output_format,
        job_name,
        job_run_id,
        table_config,
    ):
        Resource.__init__(self, database, table_name, "TARGET", catalog_id)
        self.S3_location = S3_location
        self.target_prefix = target_prefix
        self.output_format = output_format
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.table_config = table_config
        self.table_name = target_prefix + table_name


class Transform(object):
    def __init__(
        self,
        glue_client,
        source,
        target,
        glue_context,
        target_location,
        hashfield=None,
        hashpartitions=None,
    ):
        self.glue_client = glue_client
        self.source = source
        self.target = target
        self.table_exists = False
        self.glue_context = glue_context
        self.target_location = target_location
        self.hashfield = hashfield
        self.hashpartitions = hashpartitions
        self.source_schema = self.glue_client.get_table(
            CatalogId=self.source.catalog_id,
            DatabaseName=self.source.database,
            Name=self.source.table_name,
        )['Table']['StorageDescriptor']['Columns']

    def get_partition_schema(self, partition_spec):
        source_schema_map = {c['Name']: c for c in self.source_schema}
        # Partition scheme must preserve the order in partition_spec
        return [
            {'Name': source_schema_map[p]['Name'], 'Type': source_schema_map[p]['Type']}
            for p in partition_spec
        ]

    def get_schema(self, partition_spec):
        return [
            {"Name": i['Name'], "Type": i['Type']}
            for i in self.source_schema
            if i['Name'] not in partition_spec
        ]

    def get_mappings(self):
        return [(i['Name'], i['Name'], i['Type']) for i in self.source_schema]

    def get_partition_location(self, partition_spec, partition):
        return (
            self.target_location
            + self.target.table_name
            + "".join("/{}={}".format(i, partition[i]) for i in partition_spec)
            + "/"
        )

    def add_partitions(self, partition_spec, partition):
        storage_descriptor = {
            'Columns': self.get_schema(partition_spec),
            'Location': self.get_partition_location(partition_spec, partition),
            'InputFormat': '',
            'OutputFormat': '',
            'SerdeInfo': {},
        }
        if self.target.output_format.lower() == "parquet":
            storage_descriptor[
                'InputFormat'
            ] = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            storage_descriptor[
                'OutputFormat'
            ] = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            storage_descriptor['SerdeInfo'] = {
                'Parameters': {'serialization.format': '1'},
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            }

        elif self.target.output_format.lower() == "csv":
            storage_descriptor[
                'InputFormat'
            ] = "org.apache.hadoop.mapred.TextInputFormat"
            storage_descriptor[
                'OutputFormat'
            ] = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            storage_descriptor['SerdeInfo'] = {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {'field.delim': ','},
            }

        partition_input = {
            'StorageDescriptor': storage_descriptor,
            'Values': [str(i) for i in partition],
        }
        try:
            self.glue_client.create_partition(
                CatalogId=self.target.catalog_id,
                DatabaseName=self.target.database,
                TableName=self.target.table_name,
                PartitionInput=partition_input,
            )
        except Exception:
            self.glue_client.update_partition(
                CatalogId=self.target.catalog_id,
                DatabaseName=self.target.database,
                TableName=self.target.table_name,
                PartitionInput=partition_input,
                PartitionValueList=[i for i in partition],
            )
        return partition

    def _transform(self, bookmark_keys, sort_order="ASC"):
        additional_options = {
            "jobBookmarkKeys": bookmark_keys,
            "jobBookmarkKeysSortOrder": sort_order,
        }

        if self.hashfield:
            additional_options["hashfield"] = self.hashfield
        if self.hashpartitions:
            additional_options["hashpartitions"] = self.hashpartitions

        datasource0 = self.glue_context.create_dynamic_frame.from_catalog(
            database=self.source.database,
            table_name=self.source.table_name,
            catalog_id=self.source.catalog_id,
            additional_options=additional_options,
            transformation_ctx="datasource0_" + self.target.table_name,
        )

        if datasource0._rdd.take(1) == []:

            print("No new data found for table: {}".format(self.source.table_name))
            return

        applymapping1 = ApplyMapping.apply(
            frame=datasource0,
            mappings=self.get_mappings(),
            transformation_ctx="applymapping1_" + self.source.table_name,
        )

        dropnullfields2 = DropNullFields.apply(
            frame=applymapping1,
            transformation_ctx="dropnullfields2_" + self.source.table_name,
        )

        if self.source.partition_spec:
            print("[INFO]: Checking partition spec -> ", self.source.partition_spec)
            partition_values = (
                dropnullfields2.toDF()
                .select([i for i in self.source.partition_spec])
                .drop_duplicates()
                .collect()
            )
            if partition_values:
                for partition in partition_values:
                    self.add_partitions(self.source.partition_spec, partition)

        datasink3 = self.glue_context.write_dynamic_frame.from_catalog(
            frame=dropnullfields2,
            catalog_id=self.target.catalog_id,
            database=self.target.database,
            table_name=self.target.table_name,
            additional_options={"partitionKeys": self.source.partition_spec},
            transformation_ctx="datasink3_" + self.source.table_name,
        )

    def transform(self):
        self._transform(
            self.target.table_config.get('bookmark_keys'),
            self.target.table_config.get('sort_order'),
        )


class Driver(object):
    def __init__(self):
        args = utils.get_job_args(
            [
                'JOB_NAME',
                'JOB_RUN_ID',
                'WORKFLOW_NAME',
                'WORKFLOW_RUN_ID',
                'job-bookmark-option',
                'region',
                'source_table_prefix',
                'glue_endpoint',
                'catalog_id',
                'target_s3_location',
                'target_database',
                'table_config',
                'target_format',
                'job_index',
                'num_jobs',
            ],
            [
                'target_table_prefix',
                'creator_arn',
                'encryption-type',
                'hashfield',
                'hashpartitions',
            ],
        )

        self.glue_endpoint = args['glue_endpoint']
        self.region = args['region']
        self.job_name = args['JOB_NAME']
        self.job_run_id = args['JOB_RUN_ID']
        self.workflow_name = args['WORKFLOW_NAME']
        self.workflow_run_id = args['WORKFLOW_RUN_ID']

        self.creator_arn = args['creator_arn']

        self.glue_client = utils.build_glue_client(self.region, self.glue_endpoint)
        self.lf_client = utils.build_lakeformation_client(
            self.region, self.glue_endpoint
        )

        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc, minPartitions=1, targetPartitions=1)

        self.catalog_id = args['catalog_id']
        self.target_database = args['target_database']
        self.source_prefix = args['source_table_prefix']
        self.target_prefix = args['target_table_prefix']
        self.target_s3_location = args['target_s3_location']
        self.output_format = args['target_format']

        self.hashfield = None
        self.hashpartitions = None

        if 'hashfield' in args:
            self.hashfield = args['hashfield']
        if 'hashpartitions' in args:
            self.hashpartitions = args['hashpartitions']

        self.tables = self.get_tables_config(
            json.loads(args['table_config']),
            int(args['job_index']),
            int(args['num_jobs']),
        )

        self.job = Job(self.glue_context)
        self.job.init(args['JOB_NAME'], args)

    def table_exists(self, table_name):
        try:
            self.glue_client.get_table(
                CatalogId=self.catalog_id,
                DatabaseName=self.target_database,
                Name=table_name,
            )
            return True
        except:
            return False

    def should_process_table(self, job_index, num_jobs):
        def h(table_config):
            catalog_table_name = table_config['catalog_table_name']
            md5 = int(hashlib.md5(catalog_table_name.encode('utf-8')).hexdigest(), 16)
            return (md5 % num_jobs) == job_index

        return h

    def get_storage_descriptor(self, format, schema, table_name):
        if format.lower() == "parquet":
            input_format = (
                'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            )
            output_format = (
                'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            )
            serde_info = {
                'Parameters': {'serialization.format': '1'},
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            }
        elif format.lower() == "csv":
            input_format = "org.apache.hadoop.mapred.TextInputFormat"
            output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            serde_info = {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {'field.delim': ','},
            }
        elif format.lower() == "json":
            input_format = ""
            output_format = ""
            serde_info = {}
        else:
            raise Exception(
                "Format should be CSV or Parquet, but got: {}".format(format)
            )

        return {
            'Columns': schema,
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': serde_info,
            'Location': self.target_s3_location + table_name + "/",
        }

    def create_table(self, source, target, transform):
        # TODO: comment out JDBC partitioning
        schema = transform.get_schema(source.partition_spec)
        print(
            "[INFO]: Creating table with schema:",
            schema,
            "partition spec:",
            source.partition_spec,
        )
        # schema = transform.source_schema
        table_input = {
            'Name': target.table_name,
            'StorageDescriptor': self.get_storage_descriptor(
                target.output_format, schema, target.table_name
            ),
            'Parameters': {
                'classification': target.output_format,
                'SourceType': source.source_kind,
                'SourceTableName': source.table_name[len(source.source_prefix) :],
                'CreatedByJob': target.job_name,
                'CreatedByJobRun': target.job_run_id,
                'TableVersion': "0",
            },
            'TableType': 'EXTERNAL_TABLE',
        }

        if source.partition_spec:
            partition_keys = transform.get_partition_schema(source.partition_spec)
            print(
                '[INFO] Partition keys for {}: {}'.format(
                    target.table_name, partition_keys
                )
            )

            table_input['PartitionKeys'] = transform.get_partition_schema(
                source.partition_spec
            )

        get_table_result = self.glue_client.get_table(
            CatalogId=source.catalog_id,
            DatabaseName=self.target_database,
            Name=source.table_name,
        )['Table']

        table_input['Parameters']['SourceConnection'] = get_table_result['Parameters'][
            'connectionName'
        ]

        if target.output_format.lower() == "csv":
            table_input['Parameters']['skip.header.line.count'] = "1"

        self.glue_client.create_table(
            CatalogId=target.catalog_id,
            DatabaseName=target.database,
            TableInput=table_input,
        )

        target.storage_descriptor = self.get_storage_descriptor(
            target.output_format, schema, source.table_name
        )

    def update_table(self, src, tgt):
        source = self.glue_client.get_table(
            CatalogId=src.catalog_id, DatabaseName=src.database, Name=src.table_name
        )['Table']
        target = self.glue_client.get_table(
            CatalogId=tgt.catalog_id, DatabaseName=tgt.database, Name=tgt.table_name
        )['Table']

        # Compute new schema
        updated_source_schema = source['StorageDescriptor']['Columns']
        existing_target_schema = target['StorageDescriptor']['Columns']

        # Constraints:
        # Ensure the existing column order in the target does not change
        # 1. Ensure dropped columns continue to exist in the new schema (to allow querying old data)
        # 2. If a  column type is modified, keep the position but update the type

        updated_source_schema_map = {c['Name']: c for c in updated_source_schema}
        existing_target_schema_map = {c['Name']: c for c in existing_target_schema}

        # 1. Modify column types only (does not include, newly added columns)
        modified_fields = [
            updated_source_schema_map[c['Name']]
            if c['Name'] in updated_source_schema_map
            else c
            for c in existing_target_schema
        ]

        # 2. Identify newly added columns
        new_fields = [
            c
            for c in updated_source_schema
            if c['Name'] not in existing_target_schema_map
            and c['Name'] not in src.partition_spec
        ]

        updated_target_schema = modified_fields + new_fields

        # print('[INFO] Schema update summary for table {}: Old Schema - {}; New Schema: {}'.format(tgt.table_name, existing_target_schema, updated_target_schema))

        table_input = {
            key: target[key]
            for key in target
            if key not in ['CreatedBy', 'CreateTime', 'UpdateTime', 'DatabaseName']
        }
        table_input['StorageDescriptor']['Columns'] = updated_target_schema

        print(
            "[INFO] Trying to update: Target table: {}, TargetTable: {}".format(
                src.table_name, table_input['Name']
            )
        )
        self.glue_client.update_table(
            CatalogId=tgt.catalog_id, DatabaseName=tgt.database, TableInput=table_input
        )

    def update_table_job_info(self, source, target, delta, end_time):
        source = self.glue_client.get_table(
            CatalogId=target.catalog_id,
            DatabaseName=target.database,
            Name=target.table_name,
        )['Table']

        table_input = {
            key: source[key]
            for key in source
            if key not in ['CreatedBy', 'CreateTime', 'UpdateTime', 'DatabaseName']
        }
        table_input['Parameters']['LastUpdatedByJob'] = target.job_name
        table_input['Parameters']['LastUpdatedByJobRun'] = target.job_run_id
        table_input['Parameters']['TransformTime'] = delta
        table_input['Parameters']['LastTransformCompletedOn'] = end_time
        if 'TableType' not in table_input:
            table_input['TableType'] = 'EXTERNAL_TABLE'

        self.glue_client.update_table(
            CatalogId=target.catalog_id,
            DatabaseName=target.database,
            TableInput=table_input,
        )

    def get_tables_config(self, table_config, job_index, num_jobs):
        table_list = []
        next_token = ''
        while True:
            get_tables_result = self.glue_client.get_tables(
                DatabaseName=self.target_database,
                CatalogId=self.catalog_id,
                Expression="^{}.*".format(self.source_prefix),
                NextToken=next_token,
            )
            next_token = get_tables_result.get('NextToken')
            table_list.extend([t['Name'] for t in get_tables_result['TableList']])
            if not next_token:
                break
        result = []

        for i in table_config:
            conf = {}
            conf['sourceType'] = "JDBC"
            tableName = i['tableName']
            catalog_table_name = list(
                filter(lambda x: x.endswith(tableName), table_list)
            )
            if len(catalog_table_name) != 1:
                raise Exception(
                    "Could not find table {} in database {}".format(
                        tableName, self.target_database
                    )
                )
            conf['catalog_table_name'] = catalog_table_name[0]
            if "bookmarkKeys" not in i or "sortOrder" not in i:
                raise Exception(
                    "Table {} in table config missing Bookmark keys or Bookmark sort order".format(
                        catalog_table_name[0]
                    )
                )
            conf['bookmark_keys'] = i.get('bookmarkKeys')
            conf['sort_order'] = i.get('sortOrder')
            conf['partition_spec'] = i.get('partitionSpec')
            result.append(conf)

        selected_tables = list(
            filter(self.should_process_table(job_index, num_jobs), result)
        )
        print(
            "[INFO]: Will process {} tables: {}".format(
                len(selected_tables),
                [config['catalog_table_name'] for config in selected_tables],
            )
        )

        return selected_tables

    def run_transform(self):
        for i in self.tables:
            if i.get("sourceType") == "JDBC":
                source = JDBCSource(
                    self.target_database,
                    i.get('catalog_table_name'),
                    self.catalog_id,
                    self.source_prefix,
                    i.get('partition_spec'),
                )

            original_table_name = i.get('catalog_table_name')[
                len(source.source_prefix) :
            ]
            target = Target(
                self.target_database,
                original_table_name,
                self.catalog_id,
                self.target_s3_location,
                self.target_prefix,
                self.output_format,
                self.job_name,
                self.job_run_id,
                i,
            )
            transform = Transform(
                self.glue_client,
                source,
                target,
                self.glue_context,
                self.target_s3_location,
                self.hashfield,
                self.hashpartitions,
            )

            print(
                "[INFO] Source table name: {}, target table name: {}".format(
                    source.table_name, target.table_name
                )
            )
            if not self.table_exists(target.table_name):
                print(
                    "[INFO] Target Table name: {} does not exist.".format(
                        target.table_name
                    )
                )
                self.create_table(source, target, transform)
                target_table_already_exists = False
            else:
                print("[INFO] Target Table name: {} exist.".format(target.table_name))
                self.update_table(source, target)
                target_table_already_exists = True

            start_time = datetime.now()
            transform.transform()
            end_time = datetime.now()

            self.update_table_job_info(
                source, target, str(end_time - start_time), str(end_time)
            )

            # Give permissions to the table if we are just creating it for the first time
            if not target_table_already_exists:
                utils.grant_all_permission_to_creator(
                    self.lf_client,
                    target.catalog_id,
                    target.database,
                    target.table_name,
                    self.creator_arn,
                )
            else:
                print(
                    "[INFO] Not setting up permissions for the creator on the ingested table"
                )

        self.job.commit()


def main():
    driver = Driver()
    driver.run_transform()


if __name__ == "__main__":
    main()
