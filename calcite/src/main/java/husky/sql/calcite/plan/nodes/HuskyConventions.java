package husky.sql.calcite.plan.nodes;

import husky.sql.calcite.plan.nodes.logical.HuskyLogicalRel;
import husky.sql.calcite.plan.nodes.physical.HuskyPhysicalRel;
import org.apache.calcite.plan.*;

public class HuskyConventions {

    public static Convention LOGICAL = new Convention.Impl("HuskyLOGICAL", HuskyLogicalRel.class);

    public static Convention PHYSICAL = new Convention.Impl("HuskyPHYSICAL", HuskyPhysicalRel.class);
}
