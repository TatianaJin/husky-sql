package husky.sql.calcite.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
public class SimpleParquetTable extends AbstractTable implements ScannableTable {

  public String TableURL;
  public RelDataTypeFactory f;
  private String JSONTablePath;
  private List<String> ColID;
  private List<String> ColDataType;
  private final Object[][] rows = {};
  
  SimpleParquetTable(String JSONTablePath) {
    this.JSONTablePath = JSONTablePath;
    ColID = new ArrayList<String>();
    ColDataType = new ArrayList<String>();
    try{
      JSONParser parser = new JSONParser();
      JSONObject table = (JSONObject)parser.parse(new FileReader(JSONTablePath));
      this.TableURL = (String)table.get("url");
      JSONArray columns = (JSONArray)table.get("columns");
      for(Object col : columns) {
        JSONObject column = (JSONObject) col;
        String id = (String)column.get("id");
        String datatype = (String)column.get("datatype");
        ColID.add(id);
        ColDataType.add(datatype);
      }
    } catch (FileNotFoundException fe) {
      fe.printStackTrace();
    } catch (Exception e){
      e.printStackTrace();
    }
  }

  private final RelProtoDataType protoRowType = new RelProtoDataType() {
    public RelDataType apply(final RelDataTypeFactory a0) {
      Builder builder = a0.builder();
      for (int i = 0; i < ColDataType.size(); i++) {
        String colName = "col" + ColID.get(i);
        String rowType = ColDataType.get(i);
        //Col0:INT32,...
        if (rowType.equals("INT32")) {
          builder = builder.add(colName, SqlTypeName.INTEGER);
        } else if (rowType.equals("INT64")) {
          builder = builder.add(colName, SqlTypeName.BIGINT);
        } else if (rowType.equals("DOUBLE")) {
          builder = builder.add(colName, SqlTypeName.DOUBLE);
        } else if (rowType.equals("BYTE_ARRAY")) {
          builder = builder.add(colName, SqlTypeName.VARCHAR, 10);
        }
      }
      f = a0;
      return builder.build();
    }
  };

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public String toString() { return "SimpleParquetTable"; }

  public String getTableURL() { return TableURL;}

  public Enumerable<Object[]> scan(final DataContext root) {
    return Linq4j.asEnumerable(rows);
  }

}
