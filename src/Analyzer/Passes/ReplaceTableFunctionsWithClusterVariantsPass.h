#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite file() table function into fileCluster() function.
  *
  * Example: SELECT file(...) FROM distributed_table;
  * Result: SELECT fileCluster(...) FROM distributed_table;
  */
class ReplaceTableFunctionsWithClusterVariantsPass final : public IQueryTreePass
{
public:
    String getName() override { return "ReplaceTableFunctionsWithClusterVariantsPass"; }

    String getDescription() override { return "Rewrite file() function into fileCluster() function"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
