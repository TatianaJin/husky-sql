package husky.sql.calcite.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class ORCTestTableFactory implements TableFactory<ORCTestTable> {
  public ORCTestTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
  	final String directory = (String) operand.get("directory");
    return new ORCTestTable("src/main/resources/ORC_table.json");
  }
}