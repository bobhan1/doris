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

package org.apache.doris.persist.gson;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.CloudRollupJobV2;
import org.apache.doris.alter.CloudSchemaChangeJobV2;
import org.apache.doris.alter.RollupJobV2;
import org.apache.doris.alter.SchemaChangeJobV2;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.EncryptKeyRef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IPv4Literal;
import org.apache.doris.analysis.IPv6Literal;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.InformationFunction;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.JsonLiteral;
import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.analysis.LambdaFunctionExpr;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MapLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.NumericLiteralExpr;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.StructLiteral;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.analysis.VirtualSlotRef;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.AnyElementType;
import org.apache.doris.catalog.AnyStructType;
import org.apache.doris.catalog.AnyType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.HMSResource;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.MultiRowType;
import org.apache.doris.catalog.MysqlDBTable;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TemplateType;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.cloud.backup.CloudRestoreJob;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.load.CloudBrokerLoadJob;
import org.apache.doris.cloud.load.CopyJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.es.EsExternalDatabase;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergDLFExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergGlueExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergHMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergHadoopExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergRestExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergS3TablesExternalCatalog;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalDatabase;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalCatalog;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalDatabase;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalTable;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalDatabase;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.paimon.PaimonDLFExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonHMSExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalDatabase;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.fs.PersistentFileSystem;
import org.apache.doris.fs.remote.AzureFileSystem;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.ObjFileSystem;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.fs.remote.dfs.JFSFileSystem;
import org.apache.doris.fs.remote.dfs.OFSFileSystem;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.BulkLoadJob;
import org.apache.doris.load.loadv2.IngestionLoadJob;
import org.apache.doris.load.loadv2.IngestionLoadJob.IngestionLoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.InsertLoadJob;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.loadv2.MiniLoadTxnCommitAttachment;
import org.apache.doris.load.loadv2.SparkLoadJob;
import org.apache.doris.load.loadv2.SparkLoadJob.SparkLoadJobStateUpdateInfo;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadProgress;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.sync.SyncJob;
import org.apache.doris.load.sync.canal.CanalSyncJob;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.system.BrokerHbResponse;
import org.apache.doris.system.FrontendHbResponse;
import org.apache.doris.system.HeartbeatResponse;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Table;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.ReflectionAccessFilter;
import com.google.gson.ToNumberPolicy;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/*
 * Some utilities about Gson.
 * User should get GSON instance from this class to do the serialization.
 *
 *      GsonUtils.GSON.toJson(...)
 *      GsonUtils.GSON.fromJson(...)
 *
 * More example can be seen in unit test case: "org.apache.doris.common.util.GsonSerializationTest.java".
 *
 * For inherited class serialization, see "org.apache.doris.common.util.GsonDerivedClassSerializationTest.java"
 *
 * And developers may need to add other serialization adapters for custom complex java classes.
 * You need implement a class to implements JsonSerializer and JsonDeserializer, and register it to GSON_BUILDER.
 * See the following "GuavaTableAdapter" and "GuavaMultimapAdapter" for example.
 */
public class GsonUtils {
    // runtime adapter for class "Type"
    private static RuntimeTypeAdapterFactory<org.apache.doris.catalog.Type> columnTypeAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(org.apache.doris.catalog.Type.class, "clazz")
            // TODO: register other sub type after Doris support more types.
            .registerSubtype(ScalarType.class, ScalarType.class.getSimpleName())
            .registerSubtype(ArrayType.class, ArrayType.class.getSimpleName())
            .registerSubtype(MapType.class, MapType.class.getSimpleName())
            .registerSubtype(StructType.class, StructType.class.getSimpleName())
            .registerSubtype(AggStateType.class, AggStateType.class.getSimpleName())
            .registerSubtype(AnyElementType.class, AnyElementType.class.getSimpleName())
            .registerSubtype(AnyStructType.class, AnyStructType.class.getSimpleName())
            .registerSubtype(AnyType.class, AnyType.class.getSimpleName())
            .registerSubtype(MultiRowType.class, MultiRowType.class.getSimpleName())
            .registerSubtype(TemplateType.class, TemplateType.class.getSimpleName())
            .registerSubtype(VariantType.class, VariantType.class.getSimpleName());

    // runtime adapter for class "Expr"
    private static final RuntimeTypeAdapterFactory<org.apache.doris.analysis.Expr> exprAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(Expr.class, "clazz")
            .registerSubtype(FunctionCallExpr.class, FunctionCallExpr.class.getSimpleName())
            .registerSubtype(LambdaFunctionCallExpr.class, LambdaFunctionCallExpr.class.getSimpleName())
            .registerSubtype(CastExpr.class, CastExpr.class.getSimpleName())
            .registerSubtype(TimestampArithmeticExpr.class, TimestampArithmeticExpr.class.getSimpleName())
            .registerSubtype(IsNullPredicate.class, IsNullPredicate.class.getSimpleName())
            .registerSubtype(BetweenPredicate.class, BetweenPredicate.class.getSimpleName())
            .registerSubtype(BinaryPredicate.class, BinaryPredicate.class.getSimpleName())
            .registerSubtype(LikePredicate.class, LikePredicate.class.getSimpleName())
            .registerSubtype(MatchPredicate.class, MatchPredicate.class.getSimpleName())
            .registerSubtype(InPredicate.class, InPredicate.class.getSimpleName())
            .registerSubtype(CompoundPredicate.class, CompoundPredicate.class.getSimpleName())
            .registerSubtype(BoolLiteral.class, BoolLiteral.class.getSimpleName())
            .registerSubtype(MaxLiteral.class, MaxLiteral.class.getSimpleName())
            .registerSubtype(StringLiteral.class, StringLiteral.class.getSimpleName())
            .registerSubtype(IntLiteral.class, IntLiteral.class.getSimpleName())
            .registerSubtype(LargeIntLiteral.class, LargeIntLiteral.class.getSimpleName())
            .registerSubtype(LiteralExpr.class, LiteralExpr.class.getSimpleName())
            .registerSubtype(DecimalLiteral.class, DecimalLiteral.class.getSimpleName())
            .registerSubtype(FloatLiteral.class, FloatLiteral.class.getSimpleName())
            .registerSubtype(NullLiteral.class, NullLiteral.class.getSimpleName())
            .registerSubtype(MapLiteral.class, MapLiteral.class.getSimpleName())
            .registerSubtype(DateLiteral.class, DateLiteral.class.getSimpleName())
            .registerSubtype(IPv6Literal.class, IPv6Literal.class.getSimpleName())
            .registerSubtype(IPv4Literal.class, IPv4Literal.class.getSimpleName())
            .registerSubtype(JsonLiteral.class, JsonLiteral.class.getSimpleName())
            .registerSubtype(ArrayLiteral.class, ArrayLiteral.class.getSimpleName())
            .registerSubtype(StructLiteral.class, StructLiteral.class.getSimpleName())
            .registerSubtype(NumericLiteralExpr.class, NumericLiteralExpr.class.getSimpleName())
            .registerSubtype(PlaceHolderExpr.class, PlaceHolderExpr.class.getSimpleName())
            .registerSubtype(CaseExpr.class, CaseExpr.class.getSimpleName())
            .registerSubtype(LambdaFunctionExpr.class, LambdaFunctionExpr.class.getSimpleName())
            .registerSubtype(EncryptKeyRef.class, EncryptKeyRef.class.getSimpleName())
            .registerSubtype(ArithmeticExpr.class, ArithmeticExpr.class.getSimpleName())
            .registerSubtype(SlotRef.class, SlotRef.class.getSimpleName())
            .registerSubtype(VirtualSlotRef.class, VirtualSlotRef.class.getSimpleName())
            .registerSubtype(InformationFunction.class, InformationFunction.class.getSimpleName());

    // runtime adapter for class "DistributionInfo"
    private static RuntimeTypeAdapterFactory<DistributionInfo> distributionInfoTypeAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(DistributionInfo.class, "clazz")
            .registerSubtype(HashDistributionInfo.class, HashDistributionInfo.class.getSimpleName())
            .registerSubtype(RandomDistributionInfo.class, RandomDistributionInfo.class.getSimpleName());

    // runtime adapter for class "Resource"
    private static RuntimeTypeAdapterFactory<Resource> resourceTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(Resource.class, "clazz")
            .registerSubtype(SparkResource.class, SparkResource.class.getSimpleName())
            .registerSubtype(OdbcCatalogResource.class, OdbcCatalogResource.class.getSimpleName())
            .registerSubtype(S3Resource.class, S3Resource.class.getSimpleName())
            .registerSubtype(JdbcResource.class, JdbcResource.class.getSimpleName())
            .registerSubtype(HdfsResource.class, HdfsResource.class.getSimpleName())
            .registerSubtype(HMSResource.class, HMSResource.class.getSimpleName())
            .registerSubtype(EsResource.class, EsResource.class.getSimpleName());

    // runtime adapter for class "AlterJobV2"
    private static RuntimeTypeAdapterFactory<AlterJobV2> alterJobV2TypeAdapterFactory;

    static {
        alterJobV2TypeAdapterFactory = RuntimeTypeAdapterFactory.of(AlterJobV2.class, "clazz")
                .registerSubtype(CloudSchemaChangeJobV2.class, CloudSchemaChangeJobV2.class.getSimpleName())
                .registerSubtype(CloudRollupJobV2.class, CloudRollupJobV2.class.getSimpleName());
        if (Config.isNotCloudMode()) {
            alterJobV2TypeAdapterFactory
                    .registerSubtype(RollupJobV2.class, RollupJobV2.class.getSimpleName())
                    .registerSubtype(SchemaChangeJobV2.class, SchemaChangeJobV2.class.getSimpleName());
        } else {
            // compatible with old cloud code.
            alterJobV2TypeAdapterFactory
                    .registerCompatibleSubtype(CloudRollupJobV2.class, RollupJobV2.class.getSimpleName())
                    .registerCompatibleSubtype(CloudSchemaChangeJobV2.class, SchemaChangeJobV2.class.getSimpleName());
        }
    }

    // runtime adapter for class "SyncJob"
    private static RuntimeTypeAdapterFactory<SyncJob> syncJobTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(SyncJob.class, "clazz")
            .registerSubtype(CanalSyncJob.class, CanalSyncJob.class.getSimpleName());

    // runtime adapter for class "LoadJobStateUpdateInfo"
    private static RuntimeTypeAdapterFactory<LoadJobStateUpdateInfo> loadJobStateUpdateInfoTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(LoadJobStateUpdateInfo.class, "clazz")
            .registerSubtype(SparkLoadJobStateUpdateInfo.class, SparkLoadJobStateUpdateInfo.class.getSimpleName())
            .registerSubtype(IngestionLoadJobStateUpdateInfo.class,
                    IngestionLoadJobStateUpdateInfo.class.getSimpleName());

    // runtime adapter for class "Policy"
    private static RuntimeTypeAdapterFactory<Policy> policyTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    Policy.class, "clazz").registerSubtype(RowPolicy.class, RowPolicy.class.getSimpleName())
            .registerSubtype(StoragePolicy.class, StoragePolicy.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<Constraint> constraintTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    Constraint.class, "clazz")
            .registerSubtype(PrimaryKeyConstraint.class, PrimaryKeyConstraint.class.getSimpleName())
            .registerSubtype(ForeignKeyConstraint.class, ForeignKeyConstraint.class.getSimpleName())
            .registerSubtype(UniqueConstraint.class, UniqueConstraint.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<CatalogIf> dsTypeAdapterFactory;

    static {
        dsTypeAdapterFactory = RuntimeTypeAdapterFactory.of(CatalogIf.class, "clazz")
                .registerSubtype(CloudInternalCatalog.class, CloudInternalCatalog.class.getSimpleName())
                .registerSubtype(HMSExternalCatalog.class, HMSExternalCatalog.class.getSimpleName())
                .registerSubtype(EsExternalCatalog.class, EsExternalCatalog.class.getSimpleName())
                .registerSubtype(JdbcExternalCatalog.class, JdbcExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergExternalCatalog.class, IcebergExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergHMSExternalCatalog.class, IcebergHMSExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergGlueExternalCatalog.class, IcebergGlueExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergRestExternalCatalog.class, IcebergRestExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergDLFExternalCatalog.class, IcebergDLFExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergHadoopExternalCatalog.class, IcebergHadoopExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergS3TablesExternalCatalog.class,
                        IcebergS3TablesExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonExternalCatalog.class, PaimonExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonHMSExternalCatalog.class, PaimonHMSExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonFileExternalCatalog.class, PaimonFileExternalCatalog.class.getSimpleName())
                .registerSubtype(MaxComputeExternalCatalog.class, MaxComputeExternalCatalog.class.getSimpleName())
                .registerSubtype(
                            TrinoConnectorExternalCatalog.class, TrinoConnectorExternalCatalog.class.getSimpleName())
                .registerSubtype(LakeSoulExternalCatalog.class, LakeSoulExternalCatalog.class.getSimpleName())
                .registerSubtype(TestExternalCatalog.class, TestExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonDLFExternalCatalog.class, PaimonDLFExternalCatalog.class.getSimpleName());
        if (Config.isNotCloudMode()) {
            dsTypeAdapterFactory
                    .registerSubtype(InternalCatalog.class, InternalCatalog.class.getSimpleName());
        } else {
            // compatible with old cloud code.
            dsTypeAdapterFactory
                    .registerCompatibleSubtype(CloudInternalCatalog.class, InternalCatalog.class.getSimpleName());
        }
    }

    // routine load data source
    private static RuntimeTypeAdapterFactory<AbstractDataSourceProperties> rdsTypeAdapterFactory =
            RuntimeTypeAdapterFactory.of(
                            AbstractDataSourceProperties.class, "clazz")
                    .registerSubtype(KafkaDataSourceProperties.class, KafkaDataSourceProperties.class.getSimpleName());
    private static RuntimeTypeAdapterFactory<org.apache.doris.job.base.AbstractJob>
            jobExecutorRuntimeTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(org.apache.doris.job.base.AbstractJob.class, "clazz")
                            .registerSubtype(InsertJob.class, InsertJob.class.getSimpleName())
                            .registerSubtype(MTMVJob.class, MTMVJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<MTMVSnapshotIf> mtmvSnapshotTypeAdapterFactory =
            RuntimeTypeAdapterFactory.of(MTMVSnapshotIf.class, "clazz")
                    .registerSubtype(MTMVMaxTimestampSnapshot.class, MTMVMaxTimestampSnapshot.class.getSimpleName())
                    .registerSubtype(MTMVTimestampSnapshot.class, MTMVTimestampSnapshot.class.getSimpleName())
                    .registerSubtype(MTMVSnapshotIdSnapshot.class, MTMVSnapshotIdSnapshot.class.getSimpleName())
                    .registerSubtype(MTMVVersionSnapshot.class, MTMVVersionSnapshot.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<DatabaseIf> dbTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    DatabaseIf.class, "clazz")
            .registerSubtype(ExternalDatabase.class, ExternalDatabase.class.getSimpleName())
            .registerSubtype(EsExternalDatabase.class, EsExternalDatabase.class.getSimpleName())
            .registerSubtype(HMSExternalDatabase.class, HMSExternalDatabase.class.getSimpleName())
            .registerSubtype(JdbcExternalDatabase.class, JdbcExternalDatabase.class.getSimpleName())
            .registerSubtype(IcebergExternalDatabase.class, IcebergExternalDatabase.class.getSimpleName())
            .registerSubtype(LakeSoulExternalDatabase.class, LakeSoulExternalDatabase.class.getSimpleName())
            .registerSubtype(PaimonExternalDatabase.class, PaimonExternalDatabase.class.getSimpleName())
            .registerSubtype(MaxComputeExternalDatabase.class, MaxComputeExternalDatabase.class.getSimpleName())
            .registerSubtype(ExternalInfoSchemaDatabase.class, ExternalInfoSchemaDatabase.class.getSimpleName())
            .registerSubtype(ExternalMysqlDatabase.class, ExternalMysqlDatabase.class.getSimpleName())
            .registerSubtype(TrinoConnectorExternalDatabase.class, TrinoConnectorExternalDatabase.class.getSimpleName())
            .registerSubtype(TestExternalDatabase.class, TestExternalDatabase.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<TableIf> tblTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    TableIf.class, "clazz").registerSubtype(ExternalTable.class, ExternalTable.class.getSimpleName())
            .registerSubtype(EsExternalTable.class, EsExternalTable.class.getSimpleName())
            .registerSubtype(OlapTable.class, OlapTable.class.getSimpleName())
            .registerSubtype(HMSExternalTable.class, HMSExternalTable.class.getSimpleName())
            .registerSubtype(JdbcExternalTable.class, JdbcExternalTable.class.getSimpleName())
            .registerSubtype(IcebergExternalTable.class, IcebergExternalTable.class.getSimpleName())
            .registerSubtype(LakeSoulExternalTable.class, LakeSoulExternalTable.class.getSimpleName())
            .registerSubtype(PaimonExternalTable.class, PaimonExternalTable.class.getSimpleName())
            .registerSubtype(MaxComputeExternalTable.class, MaxComputeExternalTable.class.getSimpleName())
            .registerSubtype(ExternalInfoSchemaTable.class, ExternalInfoSchemaTable.class.getSimpleName())
            .registerSubtype(ExternalMysqlTable.class, ExternalMysqlTable.class.getSimpleName())
            .registerSubtype(TrinoConnectorExternalTable.class, TrinoConnectorExternalTable.class.getSimpleName())
            .registerSubtype(TestExternalTable.class, TestExternalTable.class.getSimpleName())
            .registerSubtype(BrokerTable.class, BrokerTable.class.getSimpleName())
            .registerSubtype(EsTable.class, EsTable.class.getSimpleName())
            .registerSubtype(FunctionGenTable.class, FunctionGenTable.class.getSimpleName())
            .registerSubtype(HiveTable.class, HiveTable.class.getSimpleName())
            .registerSubtype(InlineView.class, InlineView.class.getSimpleName())
            .registerSubtype(JdbcTable.class, JdbcTable.class.getSimpleName())
            .registerSubtype(MTMV.class, MTMV.class.getSimpleName())
            .registerSubtype(MysqlDBTable.class, MysqlDBTable.class.getSimpleName())
            .registerSubtype(MysqlTable.class, MysqlTable.class.getSimpleName())
            .registerSubtype(OdbcTable.class, OdbcTable.class.getSimpleName())
            .registerSubtype(SchemaTable.class, SchemaTable.class.getSimpleName())
            .registerSubtype(View.class, View.class.getSimpleName())
            .registerSubtype(Dictionary.class, Dictionary.class.getSimpleName());

    // runtime adapter for class "PartitionInfo"
    private static RuntimeTypeAdapterFactory<PartitionInfo> partitionInfoTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(PartitionInfo.class, "clazz")
            .registerSubtype(ListPartitionInfo.class, ListPartitionInfo.class.getSimpleName())
            .registerSubtype(RangePartitionInfo.class, RangePartitionInfo.class.getSimpleName())
            .registerSubtype(SinglePartitionInfo.class, SinglePartitionInfo.class.getSimpleName());

    // runtime adapter for class "HeartbeatResponse"
    private static RuntimeTypeAdapterFactory<HeartbeatResponse> hbResponseTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(HeartbeatResponse.class, "clazz")
            .registerSubtype(BackendHbResponse.class, BackendHbResponse.class.getSimpleName())
            .registerSubtype(FrontendHbResponse.class, FrontendHbResponse.class.getSimpleName())
            .registerSubtype(BrokerHbResponse.class, BrokerHbResponse.class.getSimpleName());

    // runtime adapter for class "Function"
    private static RuntimeTypeAdapterFactory<Function> functionAdapterFactory
            = RuntimeTypeAdapterFactory.of(Function.class, "clazz")
            .registerSubtype(ScalarFunction.class, ScalarFunction.class.getSimpleName())
            .registerSubtype(AggregateFunction.class, AggregateFunction.class.getSimpleName())
            .registerSubtype(AliasFunction.class, AliasFunction.class.getSimpleName());

    // runtime adapter for class "CloudReplica".
    private static RuntimeTypeAdapterFactory<Replica> replicaTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(Replica.class, "clazz")
            .registerDefaultSubtype(Replica.class)
            .registerSubtype(Replica.class, Replica.class.getSimpleName())
            .registerSubtype(CloudReplica.class, CloudReplica.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<Tablet> tabletTypeAdapterFactory;

    static {
        tabletTypeAdapterFactory = RuntimeTypeAdapterFactory
                .of(Tablet.class, "clazz")
                .registerSubtype(Tablet.class, Tablet.class.getSimpleName())
                .registerSubtype(CloudTablet.class, CloudTablet.class.getSimpleName());
        if (Config.isNotCloudMode()) {
            tabletTypeAdapterFactory.registerDefaultSubtype(Tablet.class);
        } else {
            // compatible with old cloud code.
            tabletTypeAdapterFactory.registerDefaultSubtype(CloudTablet.class);
        }
    }

    // runtime adapter for class "CloudPartition".
    private static RuntimeTypeAdapterFactory<Partition> partitionTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(Partition.class, "clazz")
            .registerDefaultSubtype(Partition.class)
            .registerSubtype(Partition.class, Partition.class.getSimpleName())
            .registerSubtype(CloudPartition.class, CloudPartition.class.getSimpleName());

    // runtime adapter for class "TxnCommitAttachment".
    private static RuntimeTypeAdapterFactory<TxnCommitAttachment> txnCommitAttachmentTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(TxnCommitAttachment.class, "clazz")
            .registerDefaultSubtype(TxnCommitAttachment.class)
            .registerSubtype(LoadJobFinalOperation.class, LoadJobFinalOperation.class.getSimpleName())
            .registerSubtype(MiniLoadTxnCommitAttachment.class, MiniLoadTxnCommitAttachment.class.getSimpleName())
            .registerSubtype(RLTaskTxnCommitAttachment.class, RLTaskTxnCommitAttachment.class.getSimpleName());

    // runtime adapter for class "RoutineLoadProgress".
    private static RuntimeTypeAdapterFactory<RoutineLoadProgress> routineLoadTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(RoutineLoadProgress.class, "clazz")
            .registerDefaultSubtype(RoutineLoadProgress.class)
            .registerSubtype(KafkaProgress.class, KafkaProgress.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<RoutineLoadJob> routineLoadJobTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(RoutineLoadJob.class, "clazz")
            .registerDefaultSubtype(RoutineLoadJob.class)
            .registerSubtype(KafkaRoutineLoadJob.class, KafkaRoutineLoadJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<PersistentFileSystem> remoteFileSystemTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(PersistentFileSystem.class, "clazz")
            .registerSubtype(BrokerFileSystem.class, BrokerFileSystem.class.getSimpleName())
            .registerSubtype(DFSFileSystem.class, DFSFileSystem.class.getSimpleName())
            .registerSubtype(JFSFileSystem.class, JFSFileSystem.class.getSimpleName())
            .registerSubtype(OFSFileSystem.class, OFSFileSystem.class.getSimpleName())
            .registerSubtype(ObjFileSystem.class, ObjFileSystem.class.getSimpleName())
            .registerSubtype(S3FileSystem.class, S3FileSystem.class.getSimpleName())
            .registerSubtype(AzureFileSystem.class, AzureFileSystem.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<org.apache.doris.backup.AbstractJob>
            jobBackupTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(org.apache.doris.backup.AbstractJob.class, "clazz")
                    .registerSubtype(BackupJob.class, BackupJob.class.getSimpleName())
                    .registerSubtype(RestoreJob.class, RestoreJob.class.getSimpleName())
                    .registerSubtype(CloudRestoreJob.class, CloudRestoreJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<LoadJob> loadJobTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(LoadJob.class, "clazz")
                    .registerSubtype(BrokerLoadJob.class, BrokerLoadJob.class.getSimpleName())
                    .registerSubtype(BulkLoadJob.class, BulkLoadJob.class.getSimpleName())
                    .registerSubtype(CloudBrokerLoadJob.class, CloudBrokerLoadJob.class.getSimpleName())
                    .registerSubtype(CopyJob.class, CopyJob.class.getSimpleName())
                    .registerSubtype(InsertLoadJob.class, InsertLoadJob.class.getSimpleName())
                    .registerSubtype(SparkLoadJob.class, SparkLoadJob.class.getSimpleName())
                    .registerSubtype(IngestionLoadJob.class, IngestionLoadJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<PartitionItem> partitionItemTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(PartitionItem.class, "clazz")
                    .registerSubtype(ListPartitionItem.class, ListPartitionItem.class.getSimpleName())
                    .registerSubtype(RangePartitionItem.class, RangePartitionItem.class.getSimpleName());

    // the builder of GSON instance.
    // Add any other adapters if necessary.
    //
    // ATTN:
    // Since GsonBuilder.create() adds all registered factories to GSON in reverse order, if you
    // need to ensure the search order of two RuntimeTypeAdapterFactory instances, be sure to
    // register them in reverse priority order.
    private static final GsonBuilder GSON_BUILDER = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .addSerializationExclusionStrategy(
                    new HiddenAnnotationExclusionStrategy()).serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization()
            .addReflectionAccessFilter(ReflectionAccessFilter.BLOCK_INACCESSIBLE_JAVA)
            .registerTypeHierarchyAdapter(Table.class, new GuavaTableAdapter())
            .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter())
            .registerTypeAdapterFactory(new PostProcessTypeAdapterFactory())
            .registerTypeAdapterFactory(new PreProcessTypeAdapterFactory())
            .registerTypeAdapterFactory(exprAdapterFactory)
            .registerTypeAdapterFactory(columnTypeAdapterFactory)
            .registerTypeAdapterFactory(distributionInfoTypeAdapterFactory)
            .registerTypeAdapterFactory(resourceTypeAdapterFactory)
            .registerTypeAdapterFactory(alterJobV2TypeAdapterFactory)
            .registerTypeAdapterFactory(syncJobTypeAdapterFactory)
            .registerTypeAdapterFactory(loadJobStateUpdateInfoTypeAdapterFactory)
            .registerTypeAdapterFactory(policyTypeAdapterFactory).registerTypeAdapterFactory(dsTypeAdapterFactory)
            .registerTypeAdapterFactory(dbTypeAdapterFactory).registerTypeAdapterFactory(tblTypeAdapterFactory)
            .registerTypeAdapterFactory(replicaTypeAdapterFactory)
            .registerTypeAdapterFactory(tabletTypeAdapterFactory)
            .registerTypeAdapterFactory(partitionTypeAdapterFactory)
            .registerTypeAdapterFactory(partitionInfoTypeAdapterFactory)
            .registerTypeAdapterFactory(hbResponseTypeAdapterFactory)
            .registerTypeAdapterFactory(functionAdapterFactory)
            .registerTypeAdapterFactory(rdsTypeAdapterFactory)
            .registerTypeAdapterFactory(jobExecutorRuntimeTypeAdapterFactory)
            .registerTypeAdapterFactory(mtmvSnapshotTypeAdapterFactory)
            .registerTypeAdapterFactory(constraintTypeAdapterFactory)
            .registerTypeAdapterFactory(txnCommitAttachmentTypeAdapterFactory)
            .registerTypeAdapterFactory(routineLoadTypeAdapterFactory)
            .registerTypeAdapterFactory(routineLoadJobTypeAdapterFactory)
            .registerTypeAdapterFactory(remoteFileSystemTypeAdapterFactory)
            .registerTypeAdapterFactory(jobBackupTypeAdapterFactory)
            .registerTypeAdapterFactory(loadJobTypeAdapterFactory)
            .registerTypeAdapterFactory(partitionItemTypeAdapterFactory)
            .registerTypeAdapter(ImmutableMap.class, new ImmutableMapDeserializer())
            .registerTypeAdapter(ImmutableList.class, new ImmutableListDeserializer())
            .registerTypeAdapter(AtomicBoolean.class, new AtomicBooleanAdapter())
            .registerTypeAdapter(PartitionKey.class, new PartitionKey.PartitionKeySerializer())
            .registerTypeAdapter(Range.class, new RangeUtils.RangeSerializer()).setExclusionStrategies(
                    new ExclusionStrategy() {
                        @Override
                        public boolean shouldSkipField(FieldAttributes f) {
                            return false;
                        }

                        @Override
                        public boolean shouldSkipClass(Class<?> clazz) {
                            /* due to java.lang.IllegalArgumentException: com.lmax.disruptor.RingBuffer
                            <org.apache.doris.scheduler.disruptor.TimerTaskEvent> declares multiple
                            JSON fields named p1 */
                            return clazz.getName().startsWith("com.lmax.disruptor.RingBuffer");
                        }
                    });


    // this instance is thread-safe.
    public static final Gson GSON = GSON_BUILDER.create();

    // ATTN: the order between creating GSON and GSON_PRETTY_PRINTING is very important.
    private static final GsonBuilder GSON_BUILDER_PRETTY_PRINTING = GSON_BUILDER.setPrettyPrinting();
    public static final Gson GSON_PRETTY_PRINTING = GSON_BUILDER_PRETTY_PRINTING.create();

    /*
     * The exclusion strategy of GSON serialization.
     * Any fields without "@SerializedName" annotation with be ignore with
     * serializing and deserializing.
     */
    public static class HiddenAnnotationExclusionStrategy implements ExclusionStrategy {
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(SerializedName.class) == null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }

    /*
     *
     * The json adapter for Guava Table.
     * Current support:
     * 1. HashBasedTable
     *
     * The RowKey, ColumnKey and Value classes in Table should also be serializable.
     *
     * What is Adapter and Why we should implement it?
     *
     * Adapter is mainly used to provide serialization and deserialization methods for some complex classes.
     * Complex classes here usually refer to classes that are complex and cannot be modified.
     * These classes mainly include third-party library classes or some inherited classes.
     */
    private static class GuavaTableAdapter<R, C, V>
            implements JsonSerializer<Table<R, C, V>>, JsonDeserializer<Table<R, C, V>> {
        /*
         * serialize Table<R, C, V> as:
         * {
         * "rowKeys": [ "rowKey1", "rowKey2", ...],
         * "columnKeys": [ "colKey1", "colKey2", ...],
         * "cells" : [[0, 0, value1], [0, 1, value2], ...]
         * }
         *
         * the [0, 0] .. in cells are the indexes of rowKeys array and columnKeys array.
         * This serialization method can reduce the size of json string because it
         * replace the same row key
         * and column key to integer.
         */
        @Override
        public JsonElement serialize(Table<R, C, V> src, Type typeOfSrc, JsonSerializationContext context) {
            JsonArray rowKeysJsonArray = new JsonArray();
            Map<R, Integer> rowKeyToIndex = new HashMap<>();
            for (R rowKey : src.rowKeySet()) {
                rowKeyToIndex.put(rowKey, rowKeyToIndex.size());
                rowKeysJsonArray.add(context.serialize(rowKey));
            }
            JsonArray columnKeysJsonArray = new JsonArray();
            Map<C, Integer> columnKeyToIndex = new HashMap<>();
            for (C columnKey : src.columnKeySet()) {
                columnKeyToIndex.put(columnKey, columnKeyToIndex.size());
                columnKeysJsonArray.add(context.serialize(columnKey));
            }
            JsonArray cellsJsonArray = new JsonArray();
            for (Table.Cell<R, C, V> cell : src.cellSet()) {
                int rowIndex = rowKeyToIndex.get(cell.getRowKey());
                int columnIndex = columnKeyToIndex.get(cell.getColumnKey());
                cellsJsonArray.add(rowIndex);
                cellsJsonArray.add(columnIndex);
                cellsJsonArray.add(context.serialize(cell.getValue()));
            }
            JsonObject tableJsonObject = new JsonObject();
            tableJsonObject.addProperty("clazz", src.getClass().getSimpleName());
            tableJsonObject.add("rowKeys", rowKeysJsonArray);
            tableJsonObject.add("columnKeys", columnKeysJsonArray);
            tableJsonObject.add("cells", cellsJsonArray);
            return tableJsonObject;
        }

        @Override
        public Table<R, C, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            Type typeOfR;
            Type typeOfC;
            Type typeOfV;
            { // CHECKSTYLE IGNORE THIS LINE
                ParameterizedType parameterizedType = (ParameterizedType) typeOfT;
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                typeOfR = actualTypeArguments[0];
                typeOfC = actualTypeArguments[1];
                typeOfV = actualTypeArguments[2];
            } // CHECKSTYLE IGNORE THIS LINE
            JsonObject tableJsonObject = json.getAsJsonObject();
            String tableClazz = tableJsonObject.get("clazz").getAsString();
            JsonArray rowKeysJsonArray = tableJsonObject.getAsJsonArray("rowKeys");
            Map<Integer, R> rowIndexToKey = new HashMap<>();
            for (JsonElement jsonElement : rowKeysJsonArray) {
                R rowKey = context.deserialize(jsonElement, typeOfR);
                rowIndexToKey.put(rowIndexToKey.size(), rowKey);
            }
            JsonArray columnKeysJsonArray = tableJsonObject.getAsJsonArray("columnKeys");
            Map<Integer, C> columnIndexToKey = new HashMap<>();
            for (JsonElement jsonElement : columnKeysJsonArray) {
                C columnKey = context.deserialize(jsonElement, typeOfC);
                columnIndexToKey.put(columnIndexToKey.size(), columnKey);
            }
            JsonArray cellsJsonArray = tableJsonObject.getAsJsonArray("cells");
            Table<R, C, V> table = null;
            switch (tableClazz) {
                case "HashBasedTable":
                    table = HashBasedTable.create();
                    break;
                default:
                    Preconditions.checkState(false, "unknown guava table class: " + tableClazz);
                    break;
            }
            for (int i = 0; i < cellsJsonArray.size(); i = i + 3) {
                // format is [rowIndex, columnIndex, value]
                int rowIndex = cellsJsonArray.get(i).getAsInt();
                int columnIndex = cellsJsonArray.get(i + 1).getAsInt();
                R rowKey = rowIndexToKey.get(rowIndex);
                C columnKey = columnIndexToKey.get(columnIndex);
                V value = context.deserialize(cellsJsonArray.get(i + 2), typeOfV);
                table.put(rowKey, columnKey, value);
            }
            return table;
        }
    }

    /*
     * The json adapter for Guava Multimap.
     * Current support:
     * 1. ArrayListMultimap
     * 2. HashMultimap
     * 3. LinkedListMultimap
     * 4. LinkedHashMultimap
     *
     * The key and value classes of multi map should also be json serializable.
     */
    private static class GuavaMultimapAdapter<K, V>
            implements JsonSerializer<Multimap<K, V>>, JsonDeserializer<Multimap<K, V>> {

        private static final Type asMapReturnType = getAsMapMethod().getGenericReturnType();

        private static Type asMapType(Type multimapType) {
            return com.google.common.reflect.TypeToken.of(multimapType).resolveType(asMapReturnType).getType();
        }

        private static Method getAsMapMethod() {
            try {
                return Multimap.class.getDeclaredMethod("asMap");
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public JsonElement serialize(Multimap<K, V> map, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("clazz", map.getClass().getSimpleName());
            Map<K, Collection<V>> asMap = map.asMap();
            Type type = asMapType(typeOfSrc);
            JsonElement jsonElement = context.serialize(asMap, type);
            jsonObject.add("map", jsonElement);
            return jsonObject;
        }

        @Override
        public Multimap<K, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String clazz = jsonObject.get("clazz").getAsString();

            JsonElement mapElement = jsonObject.get("map");
            Map<K, Collection<V>> asMap = context.deserialize(mapElement, asMapType(typeOfT));

            Multimap<K, V> map = null;
            switch (clazz) {
                case "ArrayListMultimap":
                    map = ArrayListMultimap.create();
                    break;
                case "HashMultimap":
                    map = HashMultimap.create();
                    break;
                case "LinkedListMultimap":
                    map = LinkedListMultimap.create();
                    break;
                case "LinkedHashMultimap":
                    map = LinkedHashMultimap.create();
                    break;
                default:
                    Preconditions.checkState(false, "unknown guava multi map class: " + clazz);
                    break;
            }

            for (Map.Entry<K, Collection<V>> entry : asMap.entrySet()) {
                map.putAll(entry.getKey(), entry.getValue());
            }
            return map;
        }
    }

    private static class AtomicBooleanAdapter
            implements JsonSerializer<AtomicBoolean>, JsonDeserializer<AtomicBoolean> {

        @Override
        public AtomicBoolean deserialize(JsonElement jsonElement, Type type,
                JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            boolean value = jsonObject.get("boolean").getAsBoolean();
            return new AtomicBoolean(value);
        }

        @Override
        public JsonElement serialize(AtomicBoolean atomicBoolean, Type type,
                JsonSerializationContext jsonSerializationContext) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("boolean", atomicBoolean.get());
            return jsonObject;
        }
    }

    public static final class ImmutableMapDeserializer implements JsonDeserializer<ImmutableMap<?, ?>> {
        @Override
        public ImmutableMap<?, ?> deserialize(final JsonElement json, final Type type,
                final JsonDeserializationContext context) throws JsonParseException {
            final Type type2 = TypeUtils.parameterize(Map.class, ((ParameterizedType) type).getActualTypeArguments());
            final Map<?, ?> map = context.deserialize(json, type2);
            return ImmutableMap.copyOf(map);
        }
    }

    public static final class ImmutableListDeserializer implements JsonDeserializer<ImmutableList<?>> {
        @Override
        public ImmutableList<?> deserialize(final JsonElement json, final Type type,
                final JsonDeserializationContext context) throws JsonParseException {
            final Type type2 = TypeUtils.parameterize(List.class, ((ParameterizedType) type).getActualTypeArguments());
            final List<?> list = context.deserialize(json, type2);
            return ImmutableList.copyOf(list);
        }
    }

    public static class PreProcessTypeAdapterFactory implements TypeAdapterFactory {

        public PreProcessTypeAdapterFactory() {
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

            return new TypeAdapter<T>() {
                public void write(JsonWriter out, T value) throws IOException {
                    if (value instanceof GsonPreProcessable) {
                        ((GsonPreProcessable) value).gsonPreProcess();
                    }
                    delegate.write(out, value);
                }

                public T read(JsonReader reader) throws IOException {
                    return delegate.read(reader);
                }
            };
        }
    }

    public static class PostProcessTypeAdapterFactory implements TypeAdapterFactory {

        public PostProcessTypeAdapterFactory() {
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

            return new TypeAdapter<T>() {
                public void write(JsonWriter out, T value) throws IOException {
                    delegate.write(out, value);
                }

                public T read(JsonReader reader) throws IOException {
                    T obj = delegate.read(reader);
                    if (obj instanceof GsonPostProcessable) {
                        ((GsonPostProcessable) obj).gsonPostProcess();
                    }
                    return obj;
                }
            };
        }
    }

    public static void toJsonCompressed(DataOutput out, Object src) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            try (OutputStreamWriter writer = new OutputStreamWriter(gzipStream)) {
                GsonUtils.GSON.toJson(src, writer);
            }
        }
        Text text = new Text(byteStream.toByteArray());
        text.write(out);
    }

    public static <T> T fromJsonCompressed(DataInput in, Class<T> clazz) throws IOException {
        Text text = new Text();
        text.readFields(in);

        ByteArrayInputStream byteStream = new ByteArrayInputStream(text.getBytes());
        try (GZIPInputStream gzipStream = new GZIPInputStream(byteStream)) {
            try (InputStreamReader reader = new InputStreamReader(gzipStream)) {
                return GsonUtils.GSON.fromJson(reader, clazz);
            }
        }
    }
}
