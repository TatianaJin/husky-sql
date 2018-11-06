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

public class SimpleTestTable extends AbstractTable implements ScannableTable {
  // private final Object[][] rows = {
  //     {1, "ao", 10},
  //     {2, "aka", 5},
  //     {3, "aka", 7}
  // };
  // private final RelProtoDataType protoRowType = new RelProtoDataType() {
  //   public RelDataType apply(final RelDataTypeFactory a0) {
  //     return a0.builder()
  //              .add("ID", SqlTypeName.INTEGER)
  //              .add("color", SqlTypeName.VARCHAR, 5)
  //              .add("units", SqlTypeName.INTEGER)
  //              .build();
  //   }
  // };
  private List<Object[]> rows;
  private RelProtoDataType protoRowType;

  /* From JSON*/
  private String name;
  private String type;
  private String url;
  private String factory;
  private List<Column> columns;

  public static class Column {
    private int id;
    private String name;
    private String datatype;
    private int length;

    public Column(int id, String name, String datatype) {
      this.id = id;
      this.name = name;
      this.datatype = datatype;
      this.length = 1;
    }

    /* Setters and Getters*/
    public void setId(int id){ this.id = id; }
    public void setName(String name) { this.name = name; }
    public void setDatatype(String datatype) { this.datatype = datatype; }
    public void setLength(int length) { this.length = length; }

    public int getId() { return id; }
    public String getName() { return name; }
    public String getDatatype() { return datatype; }

  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public String toString() { return "SimpleTestTable"; }

  public Enumerable<Object[]> scan(final DataContext root) {
    return Linq4j.asEnumerable(rows);
  }

  public SimpleTestTable(String tableJsonPath) {
    try{
      // JSONParser parser = new JSONParser();
      // JSONObject model = (JSONObject)parser.parse(new FileReader(modelJsonPath));
      // JSONArray schemas = (JSONArray)model.get("schemas");
      // for(Object oSchema : schemas) {
      //   JSONObject schema= (JSONObject)oSchema;
      //   JSONArray tables = (JSONArray)schema.get("tables");
      //   for(Object oTable : tables) {
          JSONParser parser = new JSONParser(); // Quetion: should we parse a table from model.json or single_table.json?
          JSONObject table = (JSONObject)parser.parse(new FileReader(tableJsonPath));
          // JSONObject table = (JSONObject)oTable;
          this.name = (String)table.get("name");
          this.type = (String)table.get("type");
          this.url = (String)table.get("url");
          this.factory = (String)table.get("factory");

          JSONArray columns = (JSONArray)table.get("columns");
          this.columns = new ArrayList<Column>();
          for(Object col : columns) {
            JSONObject column = (JSONObject) col;
            int id = Integer.parseInt((String)column.get("id"));
            String name = (String)column.get("name");
            String datatype = (String)column.get("datatype");

            Column newCol = new Column(id, name, datatype);
            if(datatype.equals("varchar")) {
              newCol.setLength(Integer.parseInt((String)column.get("length")));
            }

            this.columns.add(newCol);
          }
        // }
      // }
    } catch (FileNotFoundException fe) {
      fe.printStackTrace();
    } catch (Exception e){
      e.printStackTrace();
    }

    this.rows = new ArrayList<Object[]>();
    setRows(this.url);
    setProtoRowType();
  }

  private void setRows(String tableUrl) {
    try{
      List<List<String>> listRows = Files.lines(Paths.get(tableUrl))
                                         .map(line -> line.split("\\s+"))
                                         .map(array -> Arrays.asList(array))
                                         .collect(Collectors.toList());
      for(List<String> row : listRows) {
        List<Object> curRow = new ArrayList<Object>();
        for(int i = 0; i < row.size(); i++) {
          // set cell by datatype
          if(columns.get(i).getDatatype().equals("int")) {
            curRow.add(Integer.parseInt(row.get(i)));
          } else if(columns.get(i).getDatatype().equals("float")) {
            curRow.add(Float.parseFloat(row.get(i)));
          } else if(columns.get(i).getDatatype().equals("varchar")) {
            curRow.add(row.get(i));
          }
        }
        this.rows.add(curRow.toArray());
      }
    } catch (IOException e){
      e.printStackTrace();
    }
  }

  private SqlTypeName getColumnSqlType(String datatype) {
    if(datatype.equals("int")) {
      return SqlTypeName.INTEGER;
    } else if(datatype.equals("varchar")) {
      return SqlTypeName.VARCHAR;
    } else if(datatype.equals("float")) {
      return SqlTypeName.FLOAT;
    }

    return SqlTypeName.VARCHAR; // must return something
  }

  private void setProtoRowType() {
    protoRowType = new RelProtoDataType() {
      public RelDataType apply(final RelDataTypeFactory a0) {
        Builder builder = a0.builder();
        for(Column column : columns) {
          builder.add(column.getName(), getColumnSqlType(column.getDatatype()));
        }
        return builder.build();
      }
    };
  }
}
