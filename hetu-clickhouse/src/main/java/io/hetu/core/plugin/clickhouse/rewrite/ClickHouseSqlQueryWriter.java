/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.clickhouse.rewrite;

import com.google.common.base.Joiner;
import io.hetu.core.plugin.clickhouse.ClickHouseConfig;
import io.hetu.core.plugin.clickhouse.ClickHouseConstants;
import io.hetu.core.plugin.clickhouse.rewrite.functioncall.DateParseFunctionCallRewriter;
import io.prestosql.configmanager.ConfigSupplier;
import io.prestosql.configmanager.DefaultUdfRewriteConfigSupplier;
import io.prestosql.spi.sql.expression.Operators;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.builder.BaseSqlQueryWriter;
import io.prestosql.sql.builder.functioncall.FunctionWriterManager;
import io.prestosql.sql.builder.functioncall.FunctionWriterManagerGroup;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;
import io.prestosql.sql.builder.functioncall.functions.base.FromBase64CallRewriter;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.CHAR;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;

public class ClickHouseSqlQueryWriter
        extends BaseSqlQueryWriter
{
    private static final List<Time.ExtractField> ClickHouse_SUPPORT_EXTRACT_FIELDS_LIST = Arrays.asList(Time.ExtractField.YEAR, Time.ExtractField.MONTH, Time.ExtractField.DAY, Time.ExtractField.HOUR, Time.ExtractField.MINUTE, Time.ExtractField.SECOND);

    private FunctionWriterManager clickHouseFunctionRewriterManager;

    public ClickHouseSqlQueryWriter(ClickHouseConfig clickHouseConfig)
    {
        super();
        functionCallManagerHandle(clickHouseConfig);
    }

    private void functionCallManagerHandle(ClickHouseConfig clickHouseConfig)
    {
        ConfigSupplier configSupplier = new DefaultUdfRewriteConfigSupplier(UdfFunctionRewriteConstants.DEFAULT_VERSION_UDF_REWRITE_PATTERNS);
        DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter =
                new DefaultConnectorConfigFunctionRewriter(ClickHouseConstants.CONNECTOR_NAME, configSupplier);

        clickHouseFunctionRewriterManager = FunctionWriterManagerGroup.newFunctionWriterManagerInstance(ClickHouseConstants.CONNECTOR_NAME,
                clickHouseConfig.getClickHouseSqlVersion(), getInjectFunctionCallRewritersDefault(clickHouseConfig), connectorConfigFunctionRewriter);
    }

    private Map<String, FunctionCallRewriter> getInjectFunctionCallRewritersDefault(ClickHouseConfig clickHouseConfig)
    {
        // add the user define function re-writer
        Map<String, FunctionCallRewriter> functionCallRewriters = new HashMap<>(Collections.emptyMap());
        // 1. the base function re-writers all connector can use
        FromBase64CallRewriter fromBase64CallRewriter = new FromBase64CallRewriter();
        functionCallRewriters.put(FromBase64CallRewriter.INNER_FUNC_FROM_BASE64, fromBase64CallRewriter);

        // 2. the specific user define function re-writers
        FunctionCallRewriter unSupportedFunctionCallRewriter = new ClickHouseUnsupportedFunctionCallRewriter(ClickHouseConstants.CONNECTOR_NAME);
        functionCallRewriters.put(ClickHouseUnsupportedFunctionCallRewriter.INNER_FUNC_INTERVAL_LITERAL_DAY2SEC, unSupportedFunctionCallRewriter);
        functionCallRewriters.put(ClickHouseUnsupportedFunctionCallRewriter.INNER_FUNC_INTERVAL_LITERAL_YEAR2MONTH, unSupportedFunctionCallRewriter);
        functionCallRewriters.put(ClickHouseUnsupportedFunctionCallRewriter.INNER_FUNC_TIME_WITH_TZ_LITERAL, unSupportedFunctionCallRewriter);

        FunctionCallRewriter buildInDirectMapFunctionCallRewriter = new BuildInDirectMapFunctionCallRewriter();
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUIDLIN_AGGR_FUNC_SUM, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_AVG, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_COUNT, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_MAX, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_MIN, buildInDirectMapFunctionCallRewriter);

        FunctionCallRewriter dateParseFunctionCallRewriter = new DateParseFunctionCallRewriter();
        functionCallRewriters.put(DateParseFunctionCallRewriter.BUILD_IN_FUNC_DATE_PARSE, dateParseFunctionCallRewriter);

        return functionCallRewriters;
    }

    @Override
    public String functionCall(QualifiedName name, boolean distinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        return this.clickHouseFunctionRewriterManager.getFunctionRewriteResult(name, distinct, argumentsList, orderBy, filter, window);
    }

    /**
     * May be overridden in the future.
     */
    @Override
    public String genericLiteral(String type, String value)
    {
        // https://clickhouse.tech/docs/en/sql-reference/data-types/
        // https://clickhouse.tech/docs/en/sql-reference/syntax/
        // -- Type Constants Section
        String lowerType = type.toLowerCase(Locale.ENGLISH);
        switch (lowerType) {
            case BIGINT:
            case SMALLINT:
            case TINYINT:
            case REAL:
            case INTEGER:
                return value;
            case DOUBLE:
                return doubleLiteral(Double.parseDouble(value));
            case VARCHAR:
            case CHAR:
                return stringLiteral(value);
            default:
                String exceptionInfo = "ClickHouse Connector does not support data type " + type;
                throw new UnsupportedOperationException(exceptionInfo);
        }
    }

    @Override
    public String atTimeZone(String value, String timezone)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support at time zone");
    }

    @Override
    public String row(List<String> expressions)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support row");
    }

    @Override
    public String currentUser()
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support current user");
    }

    @Override
    public String currentPath()
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support current path");
    }

    @Override
    public String currentTime(Time.Function function, Integer precision)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support current time");
    }

    @Override
    public String extract(String expression, Time.ExtractField field)
    {
        if (ClickHouse_SUPPORT_EXTRACT_FIELDS_LIST.contains(field)) {
            return format("EXTRACT(%s FROM %s)", field, expression);
        }
        else {
            throw new UnsupportedOperationException("ClickHouse Connector does not support extract field: " + field);
        }
    }

    /**
     * To be tested.
     */
    @Override
    public String arrayConstructor(List<String> values)
    {
        return format("ARRAY(%s)", Joiner.on(", ").join(values));
    }

    @Override
    public String subscriptExpression(String base, String index)
    {
        throw new UnsupportedOperationException("ClickHouse connector does not support subscript expression");
    }

    @Override
    public String exists(String subquery)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support current exists");
    }

    @Override
    public String dereferenceExpression(String base, String field)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support dereference expression");
    }

    @Override
    public String bindExpression(List<String> values, String function)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support bind expression");
    }

    @Override
    public String comparisonExpression(Operators.ComparisonOperator operator, String left, String right)
    {
        final String[] chCompareOperators = {"==", "=", ">", "<", ">=", "<=", "!=", "<>"};
        String operatorString = operator.getValue();

        if (Arrays.asList(chCompareOperators).contains(operatorString)) {
            return format("(%s %s %s)", left, operatorString, right);
        }
        else {
            String exceptionInfo = "ClickHouse Connector does not support comparison operator " + operatorString;
            throw new UnsupportedOperationException(exceptionInfo);
        }
    }

    @Override
    public String tryExpression(String innerExpression)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support try expression");
    }

    @Override
    public String likePredicate(String value, String pattern, Optional<String> escape)
    {
        StringBuilder builder = new StringBuilder();

        builder.append('(')
                .append(value)
                .append(" LIKE ")
                .append(pattern);

        if (escape.isPresent()) {
            throw new UnsupportedOperationException("ClickHouse does not support LIKE predicate: ESCAPE clause.");
        }

        builder.append(')');

        return builder.toString();
    }

    @Override
    public String cast(String expression, String type, boolean isSafe, boolean isTypeOnly)
    {
        if (isSafe) {
            throw new UnsupportedOperationException("ClickHouse Connector does not support try_cast");
        }
        return format("CAST(%s AS %s)", expression, type);
    }

    @Override
    public String filter(String value)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support filter");
    }

    /**
     * So far, clickhouse does not support window functions.
     */
    @Override
    public String formatWindowColumn(String functionName, List<String> args, String windows)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support windows function");
    }

    @Override
    public String window(List<String> partitionBy, Optional<String> orderBy, Optional<String> frame)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support windows function");
    }

    @Override
    public String windowFrame(Types.WindowFrameType type, String start, Optional<String> end)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support windows function");
    }

    @Override
    public String groupingOperation(List<String> groupingColumns)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support grouping");
    }

    @Override
    public String aggregation(List<Selection> symbols, Optional<List<String>> groupingKeysOp, Optional<String> groupIdElementOP, String from)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(from);
        if (groupingKeysOp.isPresent()) {
            List<String> groupingKeys = groupingKeysOp.get();
            if (!groupingKeys.isEmpty()) {
                builder.append(" GROUP BY ");
                builder.append(Joiner.on(", ").join(groupingKeys));
            }
        }
        else if (groupIdElementOP.isPresent()) {
            throw new UnsupportedOperationException("ClickHouse Connector does not support grouping");
        }
        return select(symbols, builder.toString());
    }
}
