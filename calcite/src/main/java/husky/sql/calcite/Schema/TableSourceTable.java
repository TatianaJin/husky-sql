package husky.sql.calcite.Schema;


import husky.sql.calcite.Sources.TableSource;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class TableSourceTable<T> extends AbstractTable {

    TableSource<T> tableSource;
    Statistic statistic;

    public TableSourceTable(TableSource<T> tableSource, Statistic statistic) {
        this.tableSource = tableSource;
    }

    public TableSource<T> getTableSource() {
        return this.tableSource;
    }

    /**
     * Returns statistics of current table
     *
     * @return statistics of current table
     */
    @Override
    public Statistic getStatistic() {
        return this.statistic;
    }
}