package husky.sql.calcite.Sources;

import org.apache.calcite.schema.Schema;

import java.util.List;

/**
 * Defines an external table with the schema that is provided by [[TableSource#getTableSchema]].
 *
 * The data of a [[TableSource]] is produced as a [[DataSet]] in case of a [[BatchTableSource]] or
 * as a [[DataStream]] in case of a [[StreamTableSource]].
 * The type of ths produced [[DataSet]] or [[DataStream]] is specified by the
 * [[TableSource#getReturnType]] method.
 *
 * By default, the fields of the [[TableSchema]] are implicitly mapped by name to the fields of the
 * return type [[TypeInformation]]. An explicit mapping can be defined by implementing the
 * [[DefinedFieldMapping]] interface.
 *
 * @tparam T The return type of the [[TableSource]].
 */

public interface TableSource<T> {

    /** Returns the type information for the return type of the [[TableSource]].
     * The fields of the return type are mapped to the table schema based on their name.
     *
     * @return The type of the returned [[DataSet]] or [[DataStream]].
     */
    public List<Class<?>> getReturnType();

    /**
     * Returns the schema of the produced table.
     *
     * @return The [[TableSchema]] of the produced table.
     */
    public Schema getTableSchema();

    /**
     * Describes the table source.
     *
     * @return A String explaining the [[TableSource]].
     */
    public String explainSource();

}
