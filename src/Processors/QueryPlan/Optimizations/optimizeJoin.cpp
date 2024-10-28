#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Storages/StorageMemory.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>

#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <ranges>
#include <memory>



namespace DB::Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_block_size;
}

namespace DB::QueryPlanOptimizations
{

static std::optional<UInt64> estimateReadRowsCount(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        if (auto analyzed_result = reading->getAnalyzedResult())
            return analyzed_result->selected_rows;
        if (auto analyzed_result = reading->selectRangesToRead())
            return analyzed_result->selected_rows;
        return {};
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
        return reading->getStorage()->totalRows(Settings{});

    if (node.children.size() != 1)
        return {};

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return estimateReadRowsCount(*node.children.front());

    return {};
}

QueryPlan::Node * makeExpressionNodeOnTopOf(QueryPlan::Node * node, ActionsDAG actions_dag, QueryPlan::Nodes & nodes)
{
    const auto & header = node->step->getOutputHeader();
    auto step = std::make_unique<ExpressionStep>(header, std::move(actions_dag));
    return &nodes.emplace_back(QueryPlan::Node{std::move(step), {node}});
}

void optimizeJoin(QueryPlan::Node & node, QueryPlan::Nodes & nodes, bool keep_logical)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return;

    JoinActionRef left_filter(nullptr);
    JoinActionRef right_filter(nullptr);
    JoinActionRef post_filter(nullptr);
    auto join_ptr = join_step->chooseJoinAlgorithm(left_filter, right_filter, post_filter);
    if (keep_logical)
        return;

    auto & join_expression_actions = join_step->getExpressionActions();

    auto * new_left_node = makeExpressionNodeOnTopOf(node.children[0], std::move(join_expression_actions.left_pre_join_actions), nodes);
    auto * new_right_node = makeExpressionNodeOnTopOf(node.children[1], std::move(join_expression_actions.right_pre_join_actions), nodes);

    const auto & settings = join_step->getContext()->getSettingsRef();

    auto new_join_step = std::make_unique<JoinStep>(
        new_left_node->step->getOutputHeader(),
        new_right_node->step->getOutputHeader(),
        join_ptr,
        settings[Setting::max_block_size],
        settings[Setting::max_threads],
        false);

    auto & new_join_node = nodes.emplace_back();
    new_join_node.step = std::move(new_join_step);
    new_join_node.children = {new_left_node, new_right_node};

    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: {}", __FILE__, __LINE__, new_join_node.step->getOutputHeader().dumpNames());
    {
        WriteBufferFromOwnString buffer;
        IQueryPlanStep::FormatSettings settings_out{.out = buffer, .write_header = true};
        new_join_node.step->describeActions(settings_out);
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: {}", __FILE__, __LINE__, buffer.str());
    }

    node.step = std::make_unique<ExpressionStep>(new_join_node.step->getOutputHeader(), std::move(join_expression_actions.post_join_actions));
    node.children = {&new_join_node};
    // auto lhs_extimation = estimateReadRowsCount(*node.children[0]);
    // auto rhs_extimation = estimateReadRowsCount(*node.children[1]);
}

}
