package husky.sql.calcite.planner;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static org.apache.calcite.plan.Contexts.EMPTY_CONTEXT;

public class SimpleQueryPlanner {
  private final Planner planner;

  private SimpleQueryPlanner(SchemaPlus schema) {
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig calciteFrameworkConfig = configBuilder
        // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
        // case when they are read, and whether identifiers are matched case-sensitively.
        .parserConfig(
            SqlParser.configBuilder().setLex(Lex.MYSQL).build())
        .defaultSchema(schema) // Sets the schema to use by the planner
        .context(
            EMPTY_CONTEXT) // Context can store data within the planner session for access by planner rules
        .ruleSets(
            RuleSets.ofList()) // Rule sets to use in transformation phases
        .costFactory(
            null) // Custom cost factory to use during optimization
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();

    this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
  }

  public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException {
    if (args.length < 1) {
      System.out.println("usage: ./SimpleQueryPlanner \"<query string>\"");
    }
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", info)
                                                .unwrap(CalciteConnection.class);
    String schema = Resources.toString(SimpleQueryPlanner.class.getResource("/model.json"),
                                       Charset.defaultCharset());
    // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + schema);

    // Create the query planner with the toy schema
    SimpleQueryPlanner queryPlanner = new SimpleQueryPlanner(connection.getRootSchema()
                                                                       .getSubSchema(connection.getSchema()));
    RelRoot root = queryPlanner.getLogicalPlan(args[0]);
    System.out.println("Initial logical plan: ");
    System.out.println(RelOptUtil.toString(root.rel));
    System.out.println("Optimized physical plan: ");
    System.out.println(RelOptUtil.toString(queryPlanner.getPhysicalPlan(root)));
  }

  private RelRoot getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }
    SqlNode validatedSqlNode = planner.validate(sqlNode);

    return planner.rel(validatedSqlNode);
  }

  private RelNode getPhysicalPlan(RelRoot root) {
    RelNode logicalPlan = root.rel;
    RelOptPlanner planner = root.rel.getCluster().getPlanner();

    final RelVisitor visitor = new RelVisitor() {
      @Override public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
          final RelOptCluster cluster = node.getCluster();
          final RelOptTable.ToRelContext context =
              RelOptUtil.getContext(cluster);
          final RelNode r = node.getTable().toRel(context);
          planner.registerClass(r);
        }
        super.visit(node, ordinal, parent);
      }
    };
    visitor.go(logicalPlan);

    final Program program = Programs.standard();
    RelTraitSet desiredTraitSet = logicalPlan.getTraitSet()
                                             .replace(EnumerableConvention.INSTANCE)
                                             .replace(root.collation)
                                             .simplify();

    return program.run(
        planner, logicalPlan, desiredTraitSet, new ArrayList<>(), new ArrayList<>()); // no materialization or lattices
  }
}
