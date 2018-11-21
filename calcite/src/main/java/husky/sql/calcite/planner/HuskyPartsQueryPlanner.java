package husky.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;


import husky.sql.calcite.table.SimplePartsTable;
import husky.sql.calcite.table.SimplePartsTableFactory;
import husky.sql.calcite.Exceptions.TableException;
import husky.sql.calcite.plan.nodes.HuskyConventions;
import husky.sql.calcite.plan.rules.HuskyRuleSets;
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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.io.FileWriter;

import static org.apache.calcite.plan.Contexts.EMPTY_CONTEXT;

public class HuskyPartsQueryPlanner{

    private final FrameworkConfig config;


    public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException {
        if (args.length < 2) {
            System.out.println("usage: ./HuskyQueryPlanner \" JSON File \" \"<query string>\"");
        }
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", info)
                .unwrap(CalciteConnection.class);
        String schema = Resources.toString(SimpleQueryPlanner.class.getResource(args[0]),
                Charset.defaultCharset());
        // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
        new ModelHandler(connection, "inline:" + schema);

        // Create the query planner with the toy schema
        HuskyPartsQueryPlanner queryPlanner = new HuskyPartsQueryPlanner(connection.getRootSchema()
                .getSubSchema(connection.getSchema()));
        RelRoot root = queryPlanner.getLogicalPlan(args[1]);
        System.out.println("Initial logical plan: ");
        System.out.println(RelOptUtil.toString(root.rel));
        RelNode logicalPlan = queryPlanner.getPhysicalPlan(root, connection);
        System.out.println(RelOptUtil.toString(logicalPlan));

        queryPlanner.generateJSON(logicalPlan, connection);
    }





    private HuskyPartsQueryPlanner(SchemaPlus schema) {

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
     *
     * @param hepMatchOrder
     * @param ruleSet
     * @param input
     * @param targetTraits
     * @return
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


    /**
     *
     * @param volcanoPlanner
     * @param ruleSet
     * @param input
     * @param targetTraits
     * @return
     */
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


    /**
     *
     * @param query
     * @return
     * @throws ValidationException
     * @throws RelConversionException
     */
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


    /**
     *
     * @param root
     * @param connection
     * @return
     */
    private RelNode getPhysicalPlan(RelRoot root, CalciteConnection connection) {
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
        return logicalPlan;
    }



    private void generateJSON(RelNode logicalPlan, CalciteConnection connection){
        try {
            JSONObject jobj = new JSONObject();


            System.out.println("CorrelVariable: " + logicalPlan.getCorrelVariable());

            List<String> field_names = logicalPlan.getRowType().getFieldNames();

            System.out.println("\nThe field names are: ");
            JSONArray jarray = new JSONArray();

            for (String field_name : field_names) {
                System.out.println(field_name);
                jarray.add(field_name);
            }

            jobj.put("Selected Column", jarray);

        /*
        System.out.println("\nRows: ");
        System.out.println(Double.toString(logicalPlan.getRows()));
        */

        /*
        System.out.println("\nTypenames: ");
        System.out.println(logicalPlan.getRelTypeName());
        */

            RelOptTable logical_table = logicalPlan.getInput(0).getTable();

            if (logical_table == null) {
                System.out.println("The table is not accessible");
                return;
            }
            jobj.put("Recordtype", logical_table.getRowType().toString());

            System.out.println("\nHusky Record type: ");

            System.out.println(logical_table.getRowType().toString());


            String schema = connection.getSchema();

            System.out.println("\nSchema: ");
            System.out.println(schema);

            jobj.put("SCHEMA", schema);

            List<String> names =  logical_table.getQualifiedName();

            SimplePartsTable table = (SimplePartsTable) connection.getRootSchema().getSubSchema(names.get(0)).getTable(names.get(1));

            String URL = table.getUrl();

            jobj.put("URL", URL);

            for(String name: names){
                System.out.println(name);
            }

            try{
                FileWriter JSONFile = new FileWriter("src/main/resources/target.json");
                JSONFile.write(jobj.toString());
                JSONFile.flush();
            }catch (Exception e){
                System.out.println("[Error]: fail in creating json file");
            }



            System.out.println(" ");

        } catch (Exception e){
            System.out.println("[Error]: It fails in getPhysicalPlan");
        }
    }
}
