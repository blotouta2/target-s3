#!/usr/bin/env python3

import argparse
import decimal
import io
import json
import os
import pickle
import sys
import tempfile
from datetime import datetime
from os import walk

import pandas as pd
import singer
from jsonschema import Draft4Validator, FormatChecker

from target_s3 import s3
from target_s3 import utils

logger = singer.get_logger()


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def write_temp_pickle(data={}):
    temp_unique_pkl = 'temp_unique.pickle'
    dir_temp_file = os.path.join(tempfile.gettempdir(), temp_unique_pkl)
    with open(dir_temp_file, 'wb') as handle:
        pickle.dump(data, handle)


def read_temp_pickle():
    data = {}
    temp_unique_pkl = 'temp_unique.pickle'
    dir_temp_file = os.path.join(tempfile.gettempdir(), temp_unique_pkl)
    if os.path.isfile(dir_temp_file):
        with open(dir_temp_file, 'rb') as handle:
            data = pickle.load(handle)
    return data


# Upload created files to S3
def upload_to_s3(s3_client, s3_bucket, source_name, filename, stream, field_to_partition_by_time,
                 record_unique_field, compression=None, encryption_type=None, encryption_key=None):
    data = None
    df = None
    final_files_dir = ''
    with open(filename, 'r') as f:
        data = f.read().splitlines()
        df = pd.DataFrame(data)
        df.columns = ['json_element']
        df = df['json_element'].apply(json.loads)
        df = pd.json_normalize(df)
        df = df.where(pd.notnull(df), None)
        logger.info('df orginal size: {}'.format(df.shape))

    if df is not None:
        if record_unique_field and record_unique_field in df:
            unique_ids_already_processed = read_temp_pickle()
            df = df[~df[record_unique_field].isin(unique_ids_already_processed)]
            logger.info('df filtered size: {}'.format(df.shape))
            df = df.drop_duplicates()
            logger.info('df after drop_duplicates size: {}'.format(df.shape))
            # df = df.groupby(record_unique_field).first().reset_index()
            # logger.info('df first record of each unique_id size: {}'.format(df.shape))
            new_unique_ids = set(df[record_unique_field].unique())
            logger.info('unique_ids_already_processed: {}, new_unique_ids: {}'.format(
                len(unique_ids_already_processed), len(new_unique_ids)))
            unique_ids_already_processed = set(unique_ids_already_processed).union(new_unique_ids)
            write_temp_pickle(unique_ids_already_processed)

            df = df.infer_objects()
            dtypes = {}
            for c in df.columns:
                try:
                    df[c] = pd.to_numeric(df[c])
                    dtypes[str(df[c].dtype)] = dtypes.get(str(df[c].dtype), 0) + 1
                except:
                    pass
            logger.info('df info: {}'.format(dtypes))
            logger.info('df infer_objects/to_numeric size: {}'.format(df.shape))

        dir_path = os.path.dirname(os.path.realpath(filename))
        final_files_dir = os.path.join(dir_path, s3_bucket)
        final_files_dir = os.path.join(final_files_dir, stream)

        logger.info('final_files_dir: {}'.format(final_files_dir))

        if field_to_partition_by_time and field_to_partition_by_time in df:
            df['etl_run_date'] = pd.DatetimeIndex(pd.to_datetime(df[field_to_partition_by_time], format='%Y-%m-%d'))
        else:
            todayDate = datetime.now()
            df['etl_run_date'] = todayDate.strftime('%Y-%m-%d')

        for col in df.columns:
            df.rename(columns={col: utils.camel_to_snake(col)}, inplace=True)

        for col in df.columns:
            weird = (df[[col]].applymap(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
            if len(df[weird]) > 0:
                logger.info("Columns which are explicitly casted to String Type : " + str(col))
                df[col] = df[col].astype(str)

        filename_sufix_map = {'snappy': 'snappy', 'gzip': 'gz', 'brotli': 'br'}
        if compression is None or compression.lower() == "none":
            df.to_parquet(final_files_dir, engine='pyarrow', compression=None,
                          partition_cols=['etl_run_date'])
        else:
            if compression in filename_sufix_map:
                df.to_parquet(final_files_dir, engine='pyarrow', compression=compression,
                              partition_cols=['etl_run_date'])
            else:
                raise NotImplementedError("""Compression type '{}' is not supported. Expected: {}""".format(compression,
                                                                                                            filename_sufix_map.keys()))
    for (dirpath, dirnames, filenames) in walk(final_files_dir):
        for fn in filenames:
            temp_file = os.path.join(dirpath, fn)
            s3_target = os.path.join(dirpath.split(s3_bucket)[-1], fn)
            s3_target = s3_target.lstrip('/')
            s3.upload_file(temp_file,
                           s3_client,
                           s3_bucket,
                           str(source_name + '/' if source_name else '') + s3_target,
                           encryption_type=encryption_type,
                           encryption_key=encryption_key)

    # Remove the local file(s)
    for (dirpath, dirnames, filenames) in walk(final_files_dir):
        for fn in filenames:
            temp_file = os.path.join(dirpath, fn)
            os.remove(temp_file)
    os.remove(filename)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def persist_messages(messages, config, s3_client, do_timestamp_file=True):
    logger.info('persist_messages')
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    filenames = []
    filename = None
    timestamp_file_part = '-' + datetime.now().strftime('%Y%m%dT%H%M%S') if do_timestamp_file else ''
    max_file_size_mb = config.get('max_temp_file_size_mb', 100)
    stream = None

    if config.get('record_unique_field'):
        a = set()
        write_temp_pickle()

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']

        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            # Validate record
            try:
                validators[o['stream']].validate(utils.float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    logger.error("""Data validation failed and cannot load to destination. RECORD: {}\n
                    'multipleOf' validations that allows long precisions are not supported 
                    (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema.
                    """.format(o['record']))
                    raise ex

            record_to_load = o['record']
            if config.get('add_metadata_columns'):
                record_to_load = utils.add_metadata_values_to_record(o, {})
            else:
                record_to_load = utils.remove_metadata_values_from_record(o)

            flattened_record = utils.flatten(record_to_load)
            filename = o['stream'] + timestamp_file_part + '.jsonl'
            filename = os.path.join(tempfile.gettempdir(), filename)
            filename = os.path.expanduser(filename)

            if not (filename, o['stream']) in filenames:
                filenames.append((filename, o['stream']))

            with open(filename, 'a') as f:
                f.write(json.dumps(flattened_record, cls=DecimalEncoder))
                f.write('\n')

            file_size = os.path.getsize(filename) if os.path.isfile(filename) else 0
            if file_size >> 20 > max_file_size_mb:
                logger.info('file_size: {} MB, filename: {}'.format(round(file_size >> 20, 2), filename))
                upload_to_s3(s3_client, config.get("s3_bucket"), os.environ["TARGET_S3_SOURCE_NAME"], filename,
                             o['stream'],
                             config.get('field_to_partition_by_time'),
                             config.get('record_unique_field'),
                             config.get("compression"),
                             config.get('encryption_type'),
                             config.get('encryption_key'))
                filenames.remove((filename, o['stream']))
            state = None
        elif message_type == 'STATE':
            logger.info('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            if config.get('add_metadata_columns'):
                schemas[stream] = utils.add_metadata_columns_to_schema(o)

            schema = utils.float_to_decimal(o['schema'])
            validators[stream] = Draft4Validator(schema, format_checker=FormatChecker())
            key_properties[stream] = o['key_properties']
        elif message_type == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))

    # Upload created CSV files to S3
    for filename, stream in filenames:
        upload_to_s3(s3_client, config.get("s3_bucket"), os.environ["TARGET_S3_SOURCE_NAME"], filename, stream,
                     config.get('field_to_partition_by_time'),
                     config.get('record_unique_field'),
                     config.get("compression"),
                     config.get('encryption_type'),
                     config.get('encryption_key'))

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    config_errors = utils.validate_config(config)
    if len(config_errors) > 0:
        logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
        sys.exit(1)

    s3_client = s3.create_client(config)
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, config, s3_client)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
