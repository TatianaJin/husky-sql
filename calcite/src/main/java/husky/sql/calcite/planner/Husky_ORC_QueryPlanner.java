package husky.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

import husky.sql.calcite.plan.nodes.logical.HuskyLogicalCalc;
import husky.sql.calcite.Exceptions.TableException;
import husky.sql.calcite.plan.nodes.HuskyConventions;
import husky.sql.calcite.plan.rules.HuskyRuleSets;
import husky.sql.calcite.table.ORCTestTable;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static org.apache.calcite.plan.Contexts.EMPTY_CONTEXT;

public class Husky_ORC_QueryPlanner {

    private final FrameworkConfig config;
    private String[] cond_arr;

    private Husky_ORC_QueryPlanner(SchemaPlus schema) {
        config = Frameworks.newConfigBuilder()
                // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                // case when they are read, and whether identifiers are matched case-sensitively.
                .parserConfig(
                        SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(schema) // Sets the schema to use by the planner
                .context(
                        EMPTY_CONTEXT) // Context can store data within the planner session for access by planner rules
                .ruleSets(
                        RuleSets.ofList()) // Rule sets to use in transformation phases
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();
    }

    /**
     * run HEP planner
     */
    protected RelNode runHepPlanner(
            HepMatchOrder hepMatchOrder,
            RuleSet ruleSet,
            RelNode input,
            RelTraitSet targetTraits) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(hepMatchOrder);

        Iterator<RelOptRule> it = ruleSet.iterator();
        while (it.hasNext()) {
            builder.addRuleInstance(it.next());
        }

        HepPlanner planner = new HepPlanner(builder.build(), config.getContext());
        planner.setRoot(input);
        if (input.getTraitSet() != targetTraits) {
            planner.changeTraits(input, targetTraits.simplify());
        }
        return planner.findBestExp();
    }

    protected RelNode runVolcanoPlanner(
            RelOptPlanner volcanoPlanner,
            RuleSet ruleSet,
            RelNode input,
            RelTraitSet targetTraits) {
        Program optProgram = Programs.ofRules(ruleSet);

        RelNode output = null;
        try {
            output = optProgram.run(volcanoPlanner, input, targetTraits,
                    ImmutableList.of(), ImmutableList.of());
        } catch (RelOptPlanner.CannotPlanException e) {
            throw e;
//            throw new TableException(
//                    "Cannot generate a valid execution plan for the given query: \n\n" +
//                            RelOptUtil.toString(input) + "\n" +
//                            "This exception indicates that the query uses an unsupported SQL feature.\n" +
//                            "Please check the documentation for the set of currently supported SQL features.");
        } catch (TableException t) {
            throw new TableException(
                    "Cannot generate a valid execution plan for the given query: \n\n" +
                            RelOptUtil.toString(input) + "\n" +
                            t.getMessage() + "\n" +
                            "Please check the documentation for the set of currently supported SQL features.");
        } catch (AssertionError a) {
            // keep original exception stack for caller
            throw a;
        }
        return output;
    }


    private RelRoot getLogicalPlan(String query) throws ValidationException, RelConversionException {
        SqlNode sqlNode;
        Planner planner = Frameworks.getPlanner(config);

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("Query parsing error.", e);
        }
        SqlNode validatedSqlNode = planner.validate(sqlNode);

        return planner.rel(validatedSqlNode);
    }

    private RelNode getPhysicalPlan(RelRoot root) {
        RelNode originalPlan = root.rel;
        RelOptPlanner volcanoPlanner = root.rel.getCluster().getPlanner();

        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    final RelOptCluster cluster = node.getCluster();
                    final RelOptTable.ToRelContext context =
                            RelOptUtil.getContext(cluster);
                    final RelNode r = node.getTable().toRel(context);
                    volcanoPlanner.registerClass(r);
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(originalPlan);

        System.out.println("\nOriginal logical plan: ");
        System.out.println(RelOptUtil.toString(originalPlan) + "\n");

        // 0. convert sub-queries before query decorrelation
        RelNode convSubQueryPlan = runHepPlanner(
                HepMatchOrder.BOTTOM_UP, HuskyRuleSets.TABLE_SUBQUERY_RULES, originalPlan, originalPlan.getTraitSet());

        System.out.println("\nAfter step 0, convert sub-queries before query decorrelation: ");
        System.out.println(RelOptUtil.toString(convSubQueryPlan) + "\n");

        // 0. convert table references
        RelNode fullRelNode = runHepPlanner(
                HepMatchOrder.BOTTOM_UP,
                HuskyRuleSets.TABLE_REF_RULES,
                convSubQueryPlan,
                originalPlan.getTraitSet());

        System.out.println("\nAfter step 0, convert table references: ");
        System.out.println(RelOptUtil.toString(fullRelNode) + "\n");

        // 1. decorrelate
        RelNode decorPlan = RelDecorrelator.decorrelateQuery(fullRelNode);

        System.out.println("\nAfter step 1, decorrelate: ");
        System.out.println(RelOptUtil.toString(decorPlan) + "\n");

        // 2. normalize the logical plan
        RuleSet normRuleSet = HuskyRuleSets.NORM_RULES;
        RelNode normalizedPlan = decorPlan;
        if (normRuleSet.iterator().hasNext()) {
            runHepPlanner(HepMatchOrder.BOTTOM_UP, normRuleSet, decorPlan, decorPlan.getTraitSet());
        }

        System.out.println("\nAfter step 2, normalize the logical plan: ");
        System.out.println(RelOptUtil.toString(normalizedPlan) + "\n");

        // 3. optimize the logical Husky plan
        RuleSet logicalOptRuleSet = HuskyRuleSets.LOGICAL_OPT_RULES;
        RelTraitSet logicalOutputProps = originalPlan.getTraitSet().replace(HuskyConventions.LOGICAL).simplify();
        RelNode logicalPlan = normalizedPlan;
        if (logicalOptRuleSet.iterator().hasNext()) {
            logicalPlan = runVolcanoPlanner(volcanoPlanner, logicalOptRuleSet, normalizedPlan, logicalOutputProps);
        }

        System.out.println("\nAfter step 3, optimize the logical Husky plan: ");
        System.out.println(RelOptUtil.toString(logicalPlan) + "\n");

        RelOptTable logicalTable = logicalPlan.getInput(0).getTable();
        if (logicalTable == null) {
            System.out.println("table is still null");
            return logicalPlan;
        }
        return logicalPlan;
    }


    private JSONObject get_condition(List<String> exprList, String expr) {
        JSONObject cond = new JSONObject();

        StringTokenizer itr = new StringTokenizer(expr, " (,)");

        String sym = itr.nextToken();
        cond.put("operator", sym);

        int count = 1;
        while (itr.hasMoreElements()) {
            String l = itr.nextToken();
            int l_id = Integer.parseInt(l.substring(2));

            String new_l = exprList.get(l_id);
            if (new_l.startsWith("CAST")) {
                new_l = new_l.substring(4);
                StringTokenizer new_itr = new StringTokenizer(new_l, " (,)");
                new_l = new_itr.nextToken();
                l_id = Integer.parseInt(new_l.substring(2));
                new_l = exprList.get(l_id);
            }

            String ele_name = "operand " + String.valueOf(count);
            count++;
            if (new_l.contains("$t"))
                cond.put(ele_name, get_condition(exprList, new_l));
            else {
                JSONObject left_info = new JSONObject();
                left_info.put("value", new_l);
                left_info.put("id", l_id);
                cond.put(ele_name, left_info);
            }

        }
        count--;
        cond.put("num of operand", count);
        return cond;
    }

    private void generate_json(CalciteConnection conn, RelNode logicalPlan) {
        RelOptTable logicalTable = logicalPlan.getInput(0).getTable();
        List<String> names = logicalTable.getQualifiedName();
        Table table = conn.getRootSchema().getSubSchema(names.get(0)).getTable(names.get(1));
        ORCTestTable otable = (ORCTestTable) table;
        String url = otable.getUrl();
        JSONObject obj = new JSONObject();


        if (logicalPlan instanceof HuskyLogicalCalc) {
            RexProgram huskyRexProgram = ((HuskyLogicalCalc) logicalPlan).getProgram();
            List<String> filedNames = otable.getRowType(otable.factory).getFieldNames();
            List<String> exprList = new ArrayList<String>();
            for (RexNode n : huskyRexProgram.getExprList()) {
                String nString = n.toString();
                if (nString.startsWith("$")) {
                    nString = nString.substring(1);
                    nString = filedNames.get(Integer.parseInt(nString));
                }
                exprList.add(nString);
            }

            //Get projection list
            JSONArray projs = new JSONArray();
            List<String> projList = new ArrayList<String>();
            for (RexNode n : huskyRexProgram.getProjectList()) {
                String nString = n.toString();
                nString = exprList.get(Integer.parseInt(nString.substring(2)));
                projList.add(nString);
                projs.add(nString);
            }
            obj.put("projection", projs);

            if (huskyRexProgram.getCondition() != null) {
                //get conditions: $t_i
                String nString = huskyRexProgram.getCondition().toString();
                //get condition expr
                nString = exprList.get(Integer.parseInt(nString.substring(2)));
                JSONObject cond = get_condition(exprList, nString);
                obj.put("condition", cond);
            }

            obj.put("version", "1.0");
            obj.put("url", url);
            obj.put("name", "HuskyLogicalCalc");
            obj.put("type", "Calc");
        }
        try {
            FileWriter file = new FileWriter("./HuskyPhysicalPlan.json");
            file.write(obj.toJSONString());
            file.flush();
            file.close();
            System.out.println("Successfully write to json.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException {
        if (args.length < 1) {
            System.out.println("usage: ./Husky_ORC_QueryPlanner \"<query string>\"");
        }
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", info)
                .unwrap(CalciteConnection.class);
        String schema = Resources.toString(SimpleQueryPlanner.class.getResource("/sampleORC.json"),
                Charset.defaultCharset());
        // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
        new ModelHandler(connection, "inline:" + schema);

        // Create the query planner with the toy schema
        Husky_ORC_QueryPlanner queryPlanner = new Husky_ORC_QueryPlanner(connection.getRootSchema()
                .getSubSchema(connection.getSchema()));
        RelRoot root = queryPlanner.getLogicalPlan(args[0]);
        System.out.println("Initial logical plan: ");
        System.out.println(RelOptUtil.toString(root.rel));
        RelNode phyPlan = queryPlanner.getPhysicalPlan(root);
        System.out.println(RelOptUtil.toString(phyPlan));
        queryPlanner.generate_json(connection, phyPlan);
    }
}