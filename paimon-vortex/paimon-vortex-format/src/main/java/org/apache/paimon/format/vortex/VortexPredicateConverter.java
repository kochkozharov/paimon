/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import dev.vortex.api.Expression;
import dev.vortex.api.expressions.Binary;
import dev.vortex.api.expressions.GetItem;
import dev.vortex.api.expressions.Literal;
import dev.vortex.api.expressions.Not;
import dev.vortex.api.expressions.Root;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Converts Paimon {@link Predicate} to Vortex {@link Expression}. */
public class VortexPredicateConverter implements PredicateVisitor<Expression> {

    public static final VortexPredicateConverter INSTANCE = new VortexPredicateConverter();

    @Nullable
    public static Expression toVortexExpression(@Nullable List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return null;
        }
        return PredicateBuilder.and(predicates).visit(INSTANCE);
    }

    @Override
    public Expression visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOpt = predicate.fieldRefOptional();
        if (!fieldRefOpt.isPresent()) {
            return null;
        }
        FieldRef fieldRef = fieldRefOpt.get();
        Expression field = GetItem.of(Root.INSTANCE, fieldRef.name());

        // IsNull/IsNotNull сравнивают колонку с NULL. Vortex-expression
        // строго типизован — если слева `utf8?`, а справа untyped `null`,
        // native-сторона panic-ует с "Cannot compare different DTypes …/null"
        // и аборчит JVM. Строим ТИПИЗОВАННЫЙ null по типу поля; если тип
        // не поддержан (typedNull == null), скипаем pushdown — Flink
        // отфильтрует на своей стороне.
        Literal<?> typedNull = typedNullLit(fieldRef.type());
        if (predicate.function() instanceof IsNull) {
            return typedNull == null ? null : Not.of(Binary.notEq(field, typedNull));
        }
        if (predicate.function() instanceof IsNotNull) {
            return typedNull == null ? null : Binary.notEq(field, typedNull);
        }

        List<Object> literals = predicate.literals();
        if (literals == null || literals.isEmpty()) {
            return null;
        }

        Literal<?> vortexLiteral = toLiteral(fieldRef.type(), literals.get(0));
        if (vortexLiteral == null) {
            return null;
        }

        if (predicate.function() instanceof Equal) {
            return Binary.eq(field, vortexLiteral);
        } else if (predicate.function() instanceof NotEqual) {
            return Binary.notEq(field, vortexLiteral);
        } else if (predicate.function() instanceof GreaterThan) {
            return Binary.gt(field, vortexLiteral);
        } else if (predicate.function() instanceof GreaterOrEqual) {
            return Binary.gtEq(field, vortexLiteral);
        } else if (predicate.function() instanceof LessThan) {
            return Binary.lt(field, vortexLiteral);
        } else if (predicate.function() instanceof LessOrEqual) {
            return Binary.ltEq(field, vortexLiteral);
        }

        // unsupported function (e.g. In, Between)
        return null;
    }

    @Override
    public Expression visit(CompoundPredicate predicate) {
        if (predicate.function() instanceof And) {
            List<Expression> children = new ArrayList<>();
            for (Predicate child : predicate.children()) {
                Expression expr = child.visit(this);
                if (expr != null) {
                    children.add(expr);
                }
            }
            if (children.isEmpty()) {
                return null;
            }
            Expression result = children.get(0);
            for (int i = 1; i < children.size(); i++) {
                result = Binary.and(result, children.get(i));
            }
            return result;
        } else if (predicate.function() instanceof Or) {
            List<Expression> children = new ArrayList<>();
            for (Predicate child : predicate.children()) {
                Expression expr = child.visit(this);
                if (expr == null) {
                    return null;
                }
                children.add(expr);
            }
            Expression result = children.get(0);
            for (int i = 1; i < children.size(); i++) {
                result = Binary.or(result, children.get(i));
            }
            return result;
        }

        return null;
    }

    /**
     * Создаёт ТИПИЗОВАННЫЙ null-литерал по paimon-типу. Vortex-expression требует совпадения DType
     * в обеих сторонах бинарной операции — untyped {@code Literal.nullLit()} сравнивать с {@code
     * utf8?}/{@code i64?} и прочими нельзя (native panic, non-unwinding → краш TM).
     *
     * <p>Типы покрывают ВСЁ что умеет {@link #toLiteral}, плюс BINARY/VARBINARY. Для
     * неподдерживаемых типов (TIME_*, композитные ARRAY/MAP/ROW) возвращает {@code null} — caller
     * должен скипнуть pushdown.
     */
    @Nullable
    private static Literal<?> typedNullLit(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Literal.bool(null);
            case TINYINT:
                return Literal.int8(null);
            case SMALLINT:
                return Literal.int16(null);
            case INTEGER:
            case DATE:
                return Literal.int32(null);
            case BIGINT:
                return Literal.int64(null);
            case FLOAT:
                return Literal.float32(null);
            case DOUBLE:
                return Literal.float64(null);
            case CHAR:
            case VARCHAR:
                return Literal.string(null);
            case BINARY:
            case VARBINARY:
                return Literal.bytes(null);
            case DECIMAL:
                DecimalType dt = (DecimalType) type;
                return Literal.decimal(null, dt.getPrecision(), dt.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return typedNullTimestamp(((TimestampType) type).getPrecision(), Optional.empty());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return typedNullTimestamp(
                        ((LocalZonedTimestampType) type).getPrecision(), Optional.of("UTC"));
            default:
                // TIME_*, ARRAY, MAP, ROW, MULTISET — toLiteral их тоже не
                // поддерживает. Skip pushdown для таких.
                return null;
        }
    }

    private static Literal<Long> typedNullTimestamp(int precision, Optional<String> timeZone) {
        if (precision <= 3) {
            return Literal.timestampMillis(null, timeZone);
        } else if (precision <= 6) {
            return Literal.timestampMicros(null, timeZone);
        } else {
            return Literal.timestampNanos(null, timeZone);
        }
    }

    @Nullable
    private static Literal<?> toLiteral(DataType type, Object value) {
        if (value == null) {
            // Типизованный null; если тип не поддержан — вернём null и caller
            // отбросит pushdown этого предиката.
            return typedNullLit(type);
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Literal.bool((Boolean) value);
            case TINYINT:
                return Literal.int8((Byte) value);
            case SMALLINT:
                return Literal.int16((Short) value);
            case INTEGER:
            case DATE:
                return Literal.int32((Integer) value);
            case BIGINT:
                return Literal.int64((Long) value);
            case FLOAT:
                return Literal.float32((Float) value);
            case DOUBLE:
                return Literal.float64((Double) value);
            case CHAR:
            case VARCHAR:
                return Literal.string(
                        value instanceof BinaryString ? value.toString() : (String) value);
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return Literal.decimal(
                        decimal.toBigDecimal(), decimal.precision(), decimal.scale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                Timestamp ts = (Timestamp) value;
                return toTimestampLiteral(
                        ts, ((TimestampType) type).getPrecision(), Optional.empty());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp lzTs = (Timestamp) value;
                return toTimestampLiteral(
                        lzTs, ((LocalZonedTimestampType) type).getPrecision(), Optional.of("UTC"));
            default:
                return null;
        }
    }

    private static Literal<Long> toTimestampLiteral(
            Timestamp ts, int precision, Optional<String> timeZone) {
        if (precision <= 0) {
            // seconds
            return Literal.timestampMillis(ts.getMillisecond(), timeZone);
        } else if (precision <= 3) {
            // millis
            return Literal.timestampMillis(ts.getMillisecond(), timeZone);
        } else if (precision <= 6) {
            // micros
            return Literal.timestampMicros(
                    ts.getMillisecond() * 1000 + ts.getNanoOfMillisecond() / 1000, timeZone);
        } else {
            // nanos
            return Literal.timestampNanos(
                    ts.getMillisecond() * 1_000_000 + ts.getNanoOfMillisecond(), timeZone);
        }
    }
}
