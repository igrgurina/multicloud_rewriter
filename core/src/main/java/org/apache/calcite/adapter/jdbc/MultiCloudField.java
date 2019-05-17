package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptTable;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;

/**
 * <p>Copy of the Apache immutable triple consisting of three {@code Object} elements.</p>
 *
 * <p>Extended to fit into multicloud use case where I have to extract fully-qualified field information, and thus, the triple consists of Schema.Table.Field construct.</p>
 *
 * @param <L> Schema
 * @param <M> Table
 * @param <R> Field
 * @since 3.2
 */
public final class MultiCloudField<L, M, R> extends Triple<L, M, R> {

    /**
     * An immutable triple of nulls.
     */
    // This is not defined with generics to avoid warnings in call sites.
    @SuppressWarnings("rawtypes")
    private static final MultiCloudField NULL = of(null, null, null);

    /**
     * Serialization version
     */
    private static final long serialVersionUID = 1L;

    /**
     * Returns an immutable triple of nulls.
     *
     * @param <L> the left element of this triple. Value is {@code null}.
     * @param <M> the middle element of this triple. Value is {@code null}.
     * @param <R> the right element of this triple. Value is {@code null}.
     * @return an immutable triple of nulls.
     * @since 3.6
     */
    @SuppressWarnings("unchecked")
    public static <L, M, R> MultiCloudField<L, M, R> nullTriple() {
        return NULL;
    }

    /**
     * Left object
     */
    public final L schema;
    /**
     * Middle object
     */
    public final M table;
    /**
     * Right object
     */
    public final R field;

    /**
     * <p>Obtains an immutable triple of three objects inferring the generic types.</p>
     *
     * <p>This factory allows the triple to be created using inference to
     * obtain the generic types.</p>
     *
     * @param <L>    the schema element type
     * @param <M>    the table element type
     * @param <R>    the field element type
     * @param schema the schema element, may be null
     * @param table  the table element, may be null
     * @param field  the field element, may be null
     * @return a triple formed from the three parameters, not null
     */
    @SuppressWarnings("unchecked")
    public static <L, M, R> MultiCloudField<L, M, R> of(final L schema, final M table, final R field) {
        return new MultiCloudField(schema, table, field);
    }

    /**
     * Create a new triple instance.
     *
     * @param schema the schema value, may be null
     * @param table  the table value, may be null
     * @param field  the field value, may be null
     */
    public MultiCloudField(final L schema, final M table, final R field) {
        super();
        this.schema = schema;
        this.table = table;
        this.field = field;
    }

    //-----------------------------------------------------------------------

    /**
     * Returns schema.
     */
    @Override
    public L getLeft() {
        return schema;
    }

    /**
     * Returns table.
     */
    @Override
    public M getMiddle() {
        return table;
    }

    /**
     * Returns field.
     */
    @Override
    public R getRight() {
        return field;
    }

    @Override
    public String toString() {
        return this.schema + "." + this.table + "." + this.field;
    }
}

