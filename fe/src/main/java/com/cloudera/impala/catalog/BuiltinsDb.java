// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.Map;

import com.cloudera.impala.analysis.ArithmeticExpr;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CaseExpr;
import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.InPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.LikePredicate;
import com.cloudera.impala.builtins.ScalarBuiltins;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class BuiltinsDb extends Db {
  public BuiltinsDb(String name, Catalog catalog) {
    super(name, catalog);
    setIsSystemDb(true);
    initBuiltins();
  }

  /**
   * Initializes all the builtins.
   */
  private void initBuiltins() {
    // Populate all aggregate builtins.
    initAggregateBuiltins();

    // Populate all scalar builtins.
    ArithmeticExpr.initBuiltins(this);
    BinaryPredicate.initBuiltins(this);
    CastExpr.initBuiltins(this);
    CaseExpr.initBuiltins(this);
    CompoundPredicate.initBuiltins(this);
    InPredicate.initBuiltins(this);
    IsNullPredicate.initBuiltins(this);
    LikePredicate.initBuiltins(this);
    ScalarBuiltins.initBuiltins(this);
  }

  private static final Map<Type, String> SAMPLE_INIT_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "19ReservoirSampleInitIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.TINYINT,
            "19ReservoirSampleInitIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.SMALLINT,
            "19ReservoirSampleInitIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.INT,
            "19ReservoirSampleInitIN10impala_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.BIGINT,
            "19ReservoirSampleInitIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.FLOAT,
            "19ReservoirSampleInitIN10impala_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DOUBLE,
            "19ReservoirSampleInitIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.STRING,
            "19ReservoirSampleInitIN10impala_udf9StringValEEEvPNS2_15FunctionContextEPS3_")
        .put(Type.TIMESTAMP,
            "19ReservoirSampleInitIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DECIMAL,
            "19ReservoirSampleInitIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .build();

  private static final Map<Type, String> SAMPLE_SERIALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "24ReservoirSampleSerializeIN10impala_udf10BooleanValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.TINYINT,
             "24ReservoirSampleSerializeIN10impala_udf10TinyIntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.SMALLINT,
             "24ReservoirSampleSerializeIN10impala_udf11SmallIntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.INT,
             "24ReservoirSampleSerializeIN10impala_udf6IntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.BIGINT,
             "24ReservoirSampleSerializeIN10impala_udf9BigIntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.FLOAT,
             "24ReservoirSampleSerializeIN10impala_udf8FloatValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.DOUBLE,
             "24ReservoirSampleSerializeIN10impala_udf9DoubleValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.STRING,
             "24ReservoirSampleSerializeIN10impala_udf9StringValEEEKS3_PNS2_15FunctionContextERS4_")
        .put(Type.TIMESTAMP,
             "24ReservoirSampleSerializeIN10impala_udf12TimestampValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.DECIMAL,
             "24ReservoirSampleSerializeIN10impala_udf10DecimalValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .build();

  private static final Map<Type, String> SAMPLE_MERGE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "20ReservoirSampleMergeIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.TINYINT,
            "20ReservoirSampleMergeIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.SMALLINT,
            "20ReservoirSampleMergeIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.INT,
            "20ReservoirSampleMergeIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.BIGINT,
            "20ReservoirSampleMergeIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.FLOAT,
            "20ReservoirSampleMergeIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.DOUBLE,
            "20ReservoirSampleMergeIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.STRING,
            "20ReservoirSampleMergeIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKS3_PS3_")
        .put(Type.TIMESTAMP,
            "20ReservoirSampleMergeIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.DECIMAL,
            "20ReservoirSampleMergeIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .build();

  private static final Map<Type, String> SAMPLE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "21ReservoirSampleUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "21ReservoirSampleUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "21ReservoirSampleUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "21ReservoirSampleUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "21ReservoirSampleUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "21ReservoirSampleUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "21ReservoirSampleUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "21ReservoirSampleUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(Type.TIMESTAMP,
            "21ReservoirSampleUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DECIMAL,
            "21ReservoirSampleUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  private static final Map<Type, String> SAMPLE_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "23ReservoirSampleFinalizeIN10impala_udf10BooleanValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
             "23ReservoirSampleFinalizeIN10impala_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
             "23ReservoirSampleFinalizeIN10impala_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.INT,
             "23ReservoirSampleFinalizeIN10impala_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
             "23ReservoirSampleFinalizeIN10impala_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
             "23ReservoirSampleFinalizeIN10impala_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
             "23ReservoirSampleFinalizeIN10impala_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
             "23ReservoirSampleFinalizeIN10impala_udf9StringValEEES3_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
             "23ReservoirSampleFinalizeIN10impala_udf12TimestampValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
             "23ReservoirSampleFinalizeIN10impala_udf10DecimalValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .build();

  private static final Map<Type, String> APPX_MEDIAN_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "18AppxMedianFinalizeIN10impala_udf10BooleanValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
             "18AppxMedianFinalizeIN10impala_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
             "18AppxMedianFinalizeIN10impala_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.INT,
             "18AppxMedianFinalizeIN10impala_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
             "18AppxMedianFinalizeIN10impala_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
             "18AppxMedianFinalizeIN10impala_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
             "18AppxMedianFinalizeIN10impala_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
             "18AppxMedianFinalizeIN10impala_udf9StringValEEES3_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
             "18AppxMedianFinalizeIN10impala_udf12TimestampValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
             "18AppxMedianFinalizeIN10impala_udf10DecimalValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .build();

  private static final Map<Type, String> HISTOGRAM_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "17HistogramFinalizeIN10impala_udf10BooleanValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
             "17HistogramFinalizeIN10impala_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
             "17HistogramFinalizeIN10impala_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.INT,
             "17HistogramFinalizeIN10impala_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
             "17HistogramFinalizeIN10impala_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
             "17HistogramFinalizeIN10impala_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
             "17HistogramFinalizeIN10impala_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
             "17HistogramFinalizeIN10impala_udf9StringValEEES3_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
             "17HistogramFinalizeIN10impala_udf12TimestampValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
             "17HistogramFinalizeIN10impala_udf10DecimalValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .build();

  private static final Map<Type, String> HLL_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "9HllUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "9HllUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "9HllUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "9HllUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "9HllUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "9HllUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "9HllUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "9HllUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(Type.TIMESTAMP,
            "9HllUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DECIMAL,
            "9HllUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  private static final Map<Type, String> PC_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "8PcUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "8PcUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "8PcUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "8PcUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "8PcUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "8PcUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "8PcUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "8PcUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(Type.TIMESTAMP,
            "8PcUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
         .put(Type.DECIMAL,
            "8PcUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

    private static final Map<Type, String> PCSA_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
          .put(Type.BOOLEAN,
              "10PcsaUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.TINYINT,
              "10PcsaUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.SMALLINT,
              "10PcsaUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.INT,
              "10PcsaUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.BIGINT,
              "10PcsaUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.FLOAT,
              "10PcsaUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.DOUBLE,
              "10PcsaUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.STRING,
              "10PcsaUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
          .put(Type.TIMESTAMP,
              "10PcsaUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.DECIMAL,
              "10PcsaUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .build();

  private static final Map<Type, String> MIN_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "3MinIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
            "3MinIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
            "3MinIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
            "3MinIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
            "3MinIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
            "3MinIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
            "3MinIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
            "3MinIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "3MinIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "3MinIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> MAX_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "3MaxIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
            "3MaxIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
            "3MaxIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
            "3MaxIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
            "3MaxIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
            "3MaxIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
            "3MaxIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
            "3MaxIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "3MaxIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "3MaxIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> STDDEV_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.TINYINT,
            "14KnuthVarUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "14KnuthVarUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "14KnuthVarUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "14KnuthVarUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "14KnuthVarUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "14KnuthVarUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  // Populate all the aggregate builtins in the catalog.
  // null symbols indicate the function does not need that step of the evaluation.
  // An empty symbol indicates a TODO for the BE to implement the function.
  // TODO: We could also generate this in python but I'm not sure that is easier.
  private void initAggregateBuiltins() {
    final String prefix = "_ZN6impala18AggregateFunctions";
    final String initNullString = prefix +
        "14InitNullStringEPN10impala_udf15FunctionContextEPNS1_9StringValE";
    final String initNull = prefix +
        "8InitNullEPN10impala_udf15FunctionContextEPNS1_6AnyValE";
    final String stringValSerializeOrFinalize = prefix +
        "28StringValSerializeOrFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE";

    Db db = this;
    // Count (*)
    // TODO: the merge function should be Sum but the way we rewrite distincts
    // makes that not work.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
        new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        null, null, false));

    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      // Count
      db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
          Lists.newArrayList(t), Type.BIGINT, Type.BIGINT,
          prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          null, null, false));
      // Min
      String minMaxInit = t.isStringType() ? initNullString : initNull;
      String minMaxSerializeOrFinalize = t.isStringType() ?
          stringValSerializeOrFinalize : null;
      db.addBuiltin(AggregateFunction.createBuiltin(db, "min",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MIN_UPDATE_SYMBOL.get(t),
          prefix + MIN_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxSerializeOrFinalize, true));
      // Max
      db.addBuiltin(AggregateFunction.createBuiltin(db, "max",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MAX_UPDATE_SYMBOL.get(t),
          prefix + MAX_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxSerializeOrFinalize, true));

      // Sample
      db.addBuiltin(AggregateFunction.createBuiltin(db, "sample",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + SAMPLE_INIT_SYMBOL.get(t),
          prefix + SAMPLE_UPDATE_SYMBOL.get(t),
          prefix + SAMPLE_MERGE_SYMBOL.get(t),
          prefix + SAMPLE_SERIALIZE_SYMBOL.get(t),
          prefix + SAMPLE_FINALIZE_SYMBOL.get(t),
          true));

      // Approximate median
      db.addBuiltin(AggregateFunction.createBuiltin(db, "appx_median",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + SAMPLE_INIT_SYMBOL.get(t),
          prefix + SAMPLE_UPDATE_SYMBOL.get(t),
          prefix + SAMPLE_MERGE_SYMBOL.get(t),
          prefix + SAMPLE_SERIALIZE_SYMBOL.get(t),
          prefix + APPX_MEDIAN_FINALIZE_SYMBOL.get(t),
          true));

      // Histogram
      db.addBuiltin(AggregateFunction.createBuiltin(db, "histogram",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + SAMPLE_INIT_SYMBOL.get(t),
          prefix + SAMPLE_UPDATE_SYMBOL.get(t),
          prefix + SAMPLE_MERGE_SYMBOL.get(t),
          prefix + SAMPLE_SERIALIZE_SYMBOL.get(t),
          prefix + HISTOGRAM_FINALIZE_SYMBOL.get(t),
          true));

      // NDV
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "11HllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      // distinctpc
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpc",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PC_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "10PcFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      // distinctpcsa
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpcsa",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PCSA_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "12PcsaFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_samp",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_pop",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "22KnuthStddevPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_samp",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_pop",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthVarPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
      }
    }

    // Sum
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, initNull,
        prefix + "3SumIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "3SumIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, initNull,
        prefix + "3SumIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "3SumIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.<Type>newArrayList(Type.DECIMAL), Type.DECIMAL, Type.DECIMAL, initNull,
        prefix + "9SumUpdateEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        prefix + "8SumMergeEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        null, null, false));

    for (Type t: Type.getNumericTypes()) {
      // Avg
      // TODO: because of avg rewrite, BE doesn't implement it yet.
      db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
          Lists.newArrayList(t), Type.DOUBLE, Type.DOUBLE,
          "", "", "", null, "", false));
    }
    // Avg(Timestamp)
    // TODO: why does this make sense? Avg(timestamp) returns a double.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.TIMESTAMP), Type.DOUBLE, Type.DOUBLE,
        "", "", "", null, "", false));

    // Group_concat(string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING, initNullString,
        prefix +
            "18StringConcatUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "17StringConcatMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix +
            "20StringConcatFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false));
    // Group_concat(string, string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.<Type>newArrayList(Type.STRING, Type.STRING), Type.STRING, Type.STRING,
        initNullString,
        prefix +
            "18StringConcatUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValES6_PS4_",
        prefix +
            "17StringConcatMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix +
            "20StringConcatFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false));

    // analytic functions
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "rank", new ArrayList<Type>(), Type.BIGINT, Type.BIGINT));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "dense_rank", new ArrayList<Type>(), Type.BIGINT,
        Type.BIGINT));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "row_number", new ArrayList<Type>(), Type.BIGINT,
        Type.BIGINT));

    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "first_value", Lists.newArrayList(t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "last_value", Lists.newArrayList(t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lag", Lists.newArrayList(t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lag", Lists.newArrayList(t, Type.BIGINT), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lead", Lists.newArrayList(t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lead", Lists.newArrayList(t, Type.BIGINT), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t));
    }
  }
}
