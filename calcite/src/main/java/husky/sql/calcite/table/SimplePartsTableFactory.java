package husky.sql.calcite.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class SimplePartsTableFactory implements TableFactory<SimplePartsTable> {
  public SimplePartsTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
  	String tablePath = (String) operand.get("file");
  	//System.out.println(tablePath);
  	return new SimplePartsTable(tablePath);
  }
}
