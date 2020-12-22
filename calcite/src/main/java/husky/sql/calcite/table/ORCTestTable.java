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

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.io.FileNotFoundException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class ORCTestTable extends AbstractTable implements ScannableTable {


    public  RelDataTypeFactory factory;
    private List<Object[]> rows;
    private RelProtoDataType protoRowType;
    private String name;
    private String type;
    private String url;
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
  public String getUrl() {return this.url;}

  public String toString() { return "ORCTestTable"; }


  public ORCTestTable(String tableJsonPath) {
    try{
      JSONParser parser = new JSONParser();
      JSONObject table = (JSONObject)parser.parse(new FileReader(tableJsonPath));
      this.url = (String)table.get("hdfs_url");

      JSONArray columns = (JSONArray)table.get("columns");
      this.columns = new ArrayList<Column>();
      for(Object col : columns) {
        JSONObject column = (JSONObject) col;
        int id = Integer.parseInt((String)column.get("id"));
        String name = (String)column.get("name");
        String datatype = (String)column.get("d_type");

        Column newCol = new Column(id, name, datatype);
        if(datatype.equals("varchar")) {
          newCol.setLength(Integer.parseInt((String)column.get("length")));
        }
        this.columns.add(newCol);
      }
    } catch (FileNotFoundException fe) {
      fe.printStackTrace();
    } catch (Exception e){
      e.printStackTrace();
    }
    this.rows = new ArrayList<Object[]>();
    setProtoRowType();
  }



  private SqlTypeName getColumnSqlType(String datatype) {
    if(datatype.equals("int")) {
      return SqlTypeName.INTEGER;
    } else if(datatype.equals("varchar")) {
      return SqlTypeName.VARCHAR;
    } else if(datatype.equals("float")) {
      return SqlTypeName.FLOAT;
    } else if(datatype.equals("bigint")) {
        return SqlTypeName.BIGINT;
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
        factory = a0;
        return builder.build();
      }
    };
  }
    public Enumerable<Object[]> scan(final DataContext root) {
        return Linq4j.asEnumerable(rows);
    }
}