package io.tiledb.spark;

import io.tiledb.java.api.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.sources.Filter;

import java.util.HashMap;
import java.util.Map;

public class TileDBDomainPartitioner {

    /**
     * Sets a range from a filter that has been pushed down
     *
     * @param filter
     * @param domain
     * @param nonEmptyDomain
     * @throws TileDBError
     */
    private void setRangeFromFilter(Filter filter, Domain domain, HashMap<String, Pair> nonEmptyDomain) throws TileDBError {
        Map<String, Integer> dimensionIndexing = new HashMap<>();
        // Build mapping for dimension name to index
        for (int i = 0; i < domain.getNDim(); i++) {
            try (Dimension dim = domain.getDimension(i)) {
                dimensionIndexing.put(dim.getName(), i);
            }
        }
        // First handle filter that are equal so `dim = 1`
        if (filter instanceof EqualNullSafe) {
            EqualNullSafe f = (EqualNullSafe) filter;
            query.addRange(dimensionIndexing.get(f.attribute()), f.value(), f.value());
        } else if (filter instanceof EqualTo) {
            EqualTo f = (EqualTo) filter;
            query.addRange(dimensionIndexing.get(f.attribute()), f.value(), f.value());

            // GreaterThan is ranges which are in the form of `dim > 1`
        } else if (filter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) filter;
            query.addRange(
                    dimensionIndexing.get(f.attribute()),
                    addEpsilon(f.value(), domain.getType()),
                    nonEmptyDomain.get(f.attribute()).getSecond());
            // GreaterThanOrEqual is ranges which are in the form of `dim >= 1`
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
            query.addRange(
                    dimensionIndexing.get(f.attribute()),
                    f.value(),
                    nonEmptyDomain.get(f.attribute()).getSecond());

            // For in filters we will add every value as ranges of 1. `dim IN (1, 2, 3)`
        } else if (filter instanceof In) {
            In f = (In) filter;
            int dimIndex = dimensionIndexing.get(f.attribute());
            // Add every value as a new range, TileDB will collapse into super ranges for us
            for (Object value : f.values()) {
                query.addRange(dimIndex, value, value);
            }

            // LessThanl is ranges which are in the form of `dim < 1`
        } else if (filter instanceof LessThan) {
            LessThan f = (LessThan) filter;
            query.addRange(
                    dimensionIndexing.get(f.attribute()),
                    nonEmptyDomain.get(f.attribute()).getFirst(),
                    subtractEpsilon(f.value(), domain.getType()));
            // LessThanOrEqual is ranges which are in the form of `dim <= 1`
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) filter;
            query.addRange(
                    dimensionIndexing.get(f.attribute()),
                    nonEmptyDomain.get(f.attribute()).getFirst(),
                    f.value());
        } else {
            throw new TileDBError("Unsupporter filter type");
        }
    }

    /** Returns v + eps, where eps is the smallest value for the datatype such that v + eps > v. */
    private static Object addEpsilon(Object value, Datatype type) throws TileDBError {
        switch (type) {
            case TILEDB_CHAR:
            case TILEDB_INT8:
                return ((byte) value) < Byte.MAX_VALUE ? ((byte) value + 1) : value;
            case TILEDB_INT16:
                return ((short) value) < Short.MAX_VALUE ? ((short) value + 1) : value;
            case TILEDB_INT32:
                return ((int) value) < Integer.MAX_VALUE ? ((int) value + 1) : value;
            case TILEDB_INT64:
                return ((long) value) < Long.MAX_VALUE ? ((long) value + 1) : value;
            case TILEDB_UINT8:
                return ((short) value) < ((short) Byte.MAX_VALUE + 1) ? ((short) value + 1) : value;
            case TILEDB_UINT16:
                return ((int) value) < ((int) Short.MAX_VALUE + 1) ? ((int) value + 1) : value;
            case TILEDB_UINT32:
            case TILEDB_UINT64:
                return ((long) value) < ((long) Integer.MAX_VALUE + 1) ? ((long) value + 1) : value;
            case TILEDB_FLOAT32:
                return ((float) value) < Float.MAX_VALUE ? Math.nextUp((float) value) : value;
            case TILEDB_FLOAT64:
                return ((double) value) < Double.MAX_VALUE ? Math.nextUp((double) value) : value;
            default:
                throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
        }
    }

    /** Returns v - eps, where eps is the smallest value for the datatype such that v - eps < v. */
    private static Object subtractEpsilon(Object value, Datatype type) throws TileDBError {
        switch (type) {
            case TILEDB_CHAR:
            case TILEDB_INT8:
                return ((byte) value) > Byte.MIN_VALUE ? ((byte) value - 1) : value;
            case TILEDB_INT16:
                return ((short) value) > Short.MIN_VALUE ? ((short) value - 1) : value;
            case TILEDB_INT32:
                return ((int) value) > Integer.MIN_VALUE ? ((int) value - 1) : value;
            case TILEDB_INT64:
                return ((long) value) > Long.MIN_VALUE ? ((long) value - 1) : value;
            case TILEDB_UINT8:
                return ((short) value) > ((short) Byte.MIN_VALUE - 1) ? ((short) value - 1) : value;
            case TILEDB_UINT16:
                return ((int) value) > ((int) Short.MIN_VALUE - 1) ? ((int) value - 1) : value;
            case TILEDB_UINT32:
            case TILEDB_UINT64:
                return ((long) value) > ((long) Integer.MIN_VALUE - 1) ? ((long) value - 1) : value;
            case TILEDB_FLOAT32:
                return ((float) value) > Float.MIN_VALUE ? Math.nextDown((float) value) : value;
            case TILEDB_FLOAT64:
                return ((double) value) > Double.MIN_VALUE ? Math.nextDown((double) value) : value;
            default:
                throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
        }
    }

}
