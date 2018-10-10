package husky.sql.calcite.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class SimpleTestTableFactory implements TableFactory<SimpleTestTable> {
  public SimpleTestTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    return new SimpleTestTable();
  }
}
