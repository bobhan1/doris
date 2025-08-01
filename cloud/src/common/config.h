// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "configbase.h"

namespace doris::cloud::config {

CONF_Int32(brpc_listen_port, "5000");
CONF_Int32(brpc_num_threads, "64");
// connections without data transmission for so many seconds will be closed
// Set -1 to disable it.
CONF_Int32(brpc_idle_timeout_sec, "-1");
CONF_String(hostname, "");
CONF_String(fdb_cluster, "xxx:yyy@127.0.0.1:4500");
CONF_String(fdb_cluster_file_path, "./conf/fdb.cluster");
CONF_String(http_token, "greedisgood9999");
// use volatile mem kv for test. MUST NOT be `true` in production environment.
CONF_Bool(use_mem_kv, "false");
CONF_Int32(meta_server_register_interval_ms, "20000");
CONF_Int32(meta_server_lease_ms, "60000");

// for chaos testing
CONF_mBool(enable_idempotent_request_injection, "false");
// idempotent_request_replay_delay_ms = idempotent_request_replay_delay_base_ms + random(-idempotent_request_replay_delay_range_ms, idempotent_request_replay_delay_range_ms)
CONF_mInt64(idempotent_request_replay_delay_base_ms, "10000");
CONF_mInt64(idempotent_request_replay_delay_range_ms, "5000");
// exclude some request that are meaningless to replay, comma separated list. e.g. GetTabletStatsRequest,GetVersionRequest
CONF_mString(idempotent_request_replay_exclusion, "GetTabletStatsRequest,GetVersionRequest");

CONF_Int64(fdb_txn_timeout_ms, "10000");
CONF_Int64(brpc_max_body_size, "3147483648");
CONF_Int64(brpc_socket_max_unwritten_bytes, "1073741824");

CONF_String(bvar_max_dump_multi_dimension_metric_num, "5000");

// logging
CONF_String(log_dir, "./log/");
CONF_String(log_level, "info"); // info warn error fatal
CONF_Int64(log_size_mb, "1024");
CONF_Int32(log_filenum_quota, "10");
CONF_Int32(warn_log_filenum_quota, "1");
CONF_Bool(log_immediate_flush, "false");
CONF_Strings(log_verbose_modules, ""); // Comma seprated list: a.*,b.*
CONF_Int32(log_verbose_level, "5");
// Whether to use file to record log. When starting Cloud with --console,
// all logs will be written to both standard output and file.
// Disable this option will no longer use file to record log.
// Only works when starting Cloud with --console.
CONF_Bool(enable_file_logger, "true");

// Custom conf path is default empty.
// All mutable configs' modification needed to be persisted will be appended to conf path
// specified when started.
//
// If it is set to equivalent value of conf path specified when started,
// mutable configs' modification behavior will be the same as the description above.
//
// Otherwise, a new custom conf file will be created when mutable configs are modified
// and persisted, with all modification written to it.
CONF_String(custom_conf_path, "");

// recycler config
CONF_mInt64(recycle_interval_seconds, "3600");
CONF_mInt64(retention_seconds, "259200"); // 72h, global retention time
CONF_Int32(recycle_concurrency, "16");
CONF_mInt32(recycle_job_lease_expired_ms, "60000");
CONF_mInt64(compacted_rowset_retention_seconds, "1800");   // 0.5h
CONF_mInt64(dropped_index_retention_seconds, "10800");     // 3h
CONF_mInt64(dropped_partition_retention_seconds, "10800"); // 3h
// Which instance should be recycled. If empty, recycle all instances.
CONF_Strings(recycle_whitelist, ""); // Comma seprated list
// These instances will not be recycled, only effective when whitelist is empty.
CONF_Strings(recycle_blacklist, ""); // Comma seprated list
// IO worker thread pool concurrency: object list, delete
CONF_mInt32(instance_recycler_worker_pool_size, "32");
// The worker pool size for http api `statistics_recycle` worker pool
CONF_mInt32(instance_recycler_statistics_recycle_worker_pool_size, "5");
CONF_Bool(enable_checker, "false");
// The parallelism for parallel recycle operation
// s3_producer_pool recycle_tablet_pool, delete single object in this pool
CONF_Int32(recycle_pool_parallelism, "40");
// Currently only used for recycler test
CONF_Bool(enable_inverted_check, "false");
// Currently only used for recycler test
CONF_Bool(enable_delete_bitmap_inverted_check, "false");
CONF_Bool(enable_delete_bitmap_storage_optimize_v2_check, "false");
CONF_mInt64(delete_bitmap_storage_optimize_v2_check_skip_seconds, "300"); // 5min
// interval for scanning instances to do checks and inspections
CONF_mInt32(scan_instances_interval_seconds, "60"); // 1min
// interval for check object
CONF_mInt32(check_object_interval_seconds, "43200"); // 12hours
// enable recycler metrics statistics
CONF_Bool(enable_recycler_stats_metrics, "false");

CONF_mInt64(check_recycle_task_interval_seconds, "600"); // 10min
CONF_mInt64(recycler_sleep_before_scheduling_seconds, "60");
// log a warning if a recycle task takes longer than this duration
CONF_mInt64(recycle_task_threshold_seconds, "10800"); // 3h

// force recycler to recycle all useless object.
// **just for TEST**
CONF_Bool(force_immediate_recycle, "false");

CONF_mBool(enable_mow_job_key_check, "false");
CONF_mInt64(mow_job_key_check_expiration_diff_seconds, "600"); // 10min

CONF_String(test_s3_ak, "");
CONF_String(test_s3_sk, "");
CONF_String(test_s3_endpoint, "");
CONF_String(test_s3_region, "");
CONF_String(test_s3_bucket, "");
CONF_String(test_s3_prefix, "");

CONF_String(test_hdfs_prefix, "");
CONF_String(test_hdfs_fs_name, "");
// CONF_Int64(a, "1073741824");
// CONF_Bool(b, "true");

// txn config
CONF_mInt32(label_keep_max_second, "259200"); //3 * 24 * 3600 seconds
CONF_Int32(expired_txn_scan_key_nums, "1000");

// Maximum number of version of a tablet. If the version num of a tablet exceed limit,
// the load process will reject new incoming load job of this tablet.
// This is to avoid too many version num.
CONF_Int64(max_tablet_version_num, "2000");

// metrics config
CONF_Bool(use_detailed_metrics, "true");

// stage num config
CONF_Int32(max_num_stages, "40");

// qps limit config

// limit by each warehouse each rpc
CONF_Int64(default_max_qps_limit, "1000000");
// limit by each warehouse specific rpc
CONF_String(specific_max_qps_limit, "get_cluster:5000000;begin_txn:5000000");
CONF_Bool(enable_rate_limit, "true");
CONF_Int64(bvar_qps_update_second, "5");

CONF_mInt32(copy_job_max_retention_second, "259200"); //3 * 24 * 3600 seconds
CONF_String(arn_id, "");
CONF_String(arn_ak, "");
CONF_String(arn_sk, "");
CONF_Int64(internal_stage_objects_expire_time_second, "259200"); // 3 * 24 * 3600 seconds

// format with base64: eg, "cloudcloudcloudcloud" -> "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI="
CONF_String(encryption_key, "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI=");
CONF_String(encryption_method, "AES_256_ECB");

// Temporary configs for upgrade
CONF_mBool(write_schema_kv, "true");
CONF_mBool(split_tablet_stats, "true");
CONF_mBool(snapshot_get_tablet_stats, "true");

// Value codec version
CONF_mInt16(meta_schema_value_version, "1");

// Limit kv size of Schema SchemaDictKeyList, default 5MB
CONF_mInt32(schema_dict_kv_size_limit, "5242880");
// Limit the count of columns in schema dict value, default 4K
CONF_mInt32(schema_dict_key_count_limit, "4096");

// For instance check interval
CONF_Int64(reserved_buffer_days, "3");

// For recycler to do periodically log to detect alive
CONF_Int32(periodically_log_ms, "5000");

// For kms
CONF_Bool(enable_kms, "false");
CONF_String(kms_info_encryption_key, ""); // encryption_key to encrypt kms sk
CONF_String(kms_info_encryption_method, "AES_256_ECB");
// kms ak does not need to be encrypted yet, so use plaintext
CONF_String(kms_ak, "");
// kms sk uses base64-encoded ciphertext
// the plaintext must be encrypted with kms_info_encryption_key and kms_info_encryption_method
CONF_String(kms_sk, "");
CONF_String(kms_endpoint, "");
CONF_String(kms_region, "");
CONF_String(kms_provider, "ali"); // ali/tx/aws/hw, only support ali now
CONF_String(kms_cmk, "");
// When starting up, add kms data key if is missing, all MS need to be restarted simultaneously.
CONF_Bool(focus_add_kms_data_key, "false");

// Whether to retry the retryable errors that returns by the underlying txn store.
CONF_Bool(enable_txn_store_retry, "true");
// The rpc timeout of BE cloud meta mgr is set to 10s, to avoid BE rpc timeout, the retry time here
// should satisfy that:
//  (1 << txn_store_retry_times) * txn_store_retry_base_internvals_ms < 10s
CONF_Int32(txn_store_retry_times, "4");
CONF_Int32(txn_store_retry_base_intervals_ms, "500");
// Whether to retry the txn conflict errors that returns by the underlying txn store.
CONF_Bool(enable_retry_txn_conflict, "true");

CONF_mBool(enable_s3_rate_limiter, "false");
CONF_mInt64(s3_get_bucket_tokens, "1000000000000000000");
CONF_Validator(s3_get_bucket_tokens, [](int64_t config) -> bool { return config > 0; });

CONF_mInt64(s3_get_token_per_second, "1000000000000000000");
CONF_Validator(s3_get_token_per_second, [](int64_t config) -> bool { return config > 0; });
CONF_mInt64(s3_get_token_limit, "0");

CONF_mInt64(s3_put_bucket_tokens, "1000000000000000000");
CONF_Validator(s3_put_bucket_tokens, [](int64_t config) -> bool { return config > 0; });
CONF_mInt64(s3_put_token_per_second, "1000000000000000000");
CONF_Validator(s3_put_token_per_second, [](int64_t config) -> bool { return config > 0; });
CONF_mInt64(s3_put_token_limit, "0");

// The secondary package name of the MetaService.
CONF_String(secondary_package_name, "");

// Allow to specify kerberos credentials cache path.
CONF_String(kerberos_ccache_path, "");
// set krb5.conf path, use "/etc/krb5.conf" by default
CONF_String(kerberos_krb5_conf_path, "/etc/krb5.conf");

CONF_mBool(enable_distinguish_hdfs_path, "true");

// If enabled, the txn status will be checked when preapre/commit rowset
CONF_mBool(enable_load_txn_status_check, "true");

CONF_mBool(enable_tablet_job_check, "true");

// Declare a selection strategy for those servers have many ips.
// Note that there should at most one ip match this list.
// this is a list in semicolon-delimited format, in CIDR notation,
// e.g. 10.10.10.0/24
// e.g. 10.10.10.0/24;192.168.0.1/24
// If no IP match this rule, a random IP is used (usually it is the IP binded to hostname).
CONF_String(priority_networks, "");

CONF_Bool(enable_cluster_name_check, "false");

// http scheme in S3Client to use. E.g. http or https
CONF_String(s3_client_http_scheme, "http");
CONF_Validator(s3_client_http_scheme, [](const std::string& config) -> bool {
    return config == "http" || config == "https";
});

CONF_Bool(force_azure_blob_global_endpoint, "false");

// Max retry times for object storage request
CONF_mInt64(max_s3_client_retry, "10");

// Max byte getting delete bitmap can return, default is 1GB
CONF_mInt64(max_get_delete_bitmap_byte, "1073741824");
// retry configs of remove_delete_bitmap_update_lock txn_conflict
CONF_Bool(delete_bitmap_enable_retry_txn_conflict, "true");

// Max byte txn commit when updating delete bitmap, default is 7MB.
// Because the size of one fdb transaction can't exceed 10MB, and
// fdb does not have an accurate way to estimate the size of txn.
// In my test, when txn->approximate_bytes() bigger than 8MB,
// it may meet Transaction exceeds byte limit error. We'd better
// reserve 1MB of buffer, so setting the default value to 7MB is
// more reasonable.
CONF_mInt64(max_txn_commit_byte, "7340032");

CONF_Bool(enable_cloud_txn_lazy_commit, "true");
CONF_Int32(txn_lazy_commit_rowsets_thresold, "1000");
CONF_Int32(txn_lazy_commit_num_threads, "8");
CONF_Int32(txn_lazy_max_rowsets_per_batch, "1000");
// max TabletIndexPB num for batch get
CONF_Int32(max_tablet_index_num_per_batch, "1000");
CONF_Int32(max_restore_job_rowsets_per_batch, "1000");

CONF_Bool(enable_cloud_txn_lazy_commit_fuzzy_test, "false");

// Max aborted txn num for the same label name
CONF_mInt64(max_num_aborted_txn, "100");

CONF_Bool(enable_check_instance_id, "true");

// Check if ip eq 127.0.0.1, ms/recycler exit
CONF_Bool(enable_loopback_address_for_ms, "false");

// delete_bitmap_lock version config
// here is some examples:
// 1. If instance1,instance2 use v2, config should be
// delete_bitmap_lock_v2_white_list = instance1;instance2
// 2. If all instance use v2, config should be
// delete_bitmap_lock_v2_white_list = *
CONF_mString(delete_bitmap_lock_v2_white_list, "");
// FOR DEBUGGING
CONF_mBool(use_delete_bitmap_lock_random_version, "false");

// Which vaults should be recycled. If empty, recycle all vaults.
// Comma seprated list: recycler_storage_vault_white_list="aaa,bbb,ccc"
CONF_Strings(recycler_storage_vault_white_list, "");

// for test only
CONF_mBool(enable_update_delete_bitmap_kv_check, "false");

// for get_delete_bitmap_update_lock
CONF_mBool(enable_batch_get_mow_tablet_stats_and_meta, "true");

// aws sdk log level
//    Off = 0,
//    Fatal = 1,
//    Error = 2,
//    Warn = 3,
//    Info = 4,
//    Debug = 5,
//    Trace = 6
CONF_Int32(aws_log_level, "3");
CONF_Validator(aws_log_level, [](const int config) -> bool { return config >= 0 && config <= 6; });

// azure sdk log level
//    Verbose = 1,
//    Informational = 2,
//    Warning = 3,
//    Error = 4
CONF_Int32(azure_log_level, "3");
CONF_Validator(azure_log_level,
               [](const int config) -> bool { return config >= 1 && config <= 4; });

// ca_cert_file is in this path by default, Normally no modification is required
// ca cert default path is different from different OS
CONF_mString(ca_cert_file_paths,
             "/etc/pki/tls/certs/ca-bundle.crt;/etc/ssl/certs/ca-certificates.crt;"
             "/etc/ssl/ca-bundle.pem");

CONF_Bool(enable_split_rowset_meta_pb, "false");
CONF_Int32(split_rowset_meta_pb_size, "10000"); // split rowset meta pb size, default is 10K
CONF_Bool(enable_check_fe_drop_in_safe_time, "true");

} // namespace doris::cloud::config
