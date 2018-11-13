package husky.sql.calcite.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class SimpleParquetTableFactory implements TableFactory<SimpleParquetTable> {
  public SimpleParquetTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    String JSONTablePath = (String) operand.get("file");
    return new SimpleParquetTable(JSONTablePath);
  }
}
