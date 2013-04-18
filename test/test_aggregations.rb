require 'test/unit'
require 'cascading'
require 'cascading/sub_assembly'

require 'test/mock_assemblies'

class TC_Aggregations < Test::Unit::TestCase
  include MockAssemblies

  # first chosen because it does not have a corresponding AggregateBy
  def test_create_group_by
    group = nil
    assembly = mock_assembly do
      group = group_by 'line' do
        first 'offset'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::First, assembly.tail_pipe.aggregator.class

    grouping_fields = group.key_selectors['test']
    assert_equal ['line'], grouping_fields.to_a

    assert_equal ['line', 'offset'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'offset'], assembly.scope.grouping_fields.to_a

    assembly = mock_assembly do
      group = group_by 'offset' do
        first 'line'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::First, assembly.tail_pipe.aggregator.class

    grouping_fields = group.key_selectors['test']
    assert_equal ['offset'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_aggregate_by
    group = nil
    assembly = mock_assembly do
      group = group_by 'line' do
        count
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is a Sum, not a Count
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Sum, assembly.tail_pipe.aggregator.class

    assert_equal ['line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'count'], assembly.scope.grouping_fields.to_a

    assembly = mock_assembly do
      group = group_by 'offset' do
        count
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is a Sum, not a Count
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Sum, assembly.tail_pipe.aggregator.class

    assert_equal ['offset', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'count'], assembly.scope.grouping_fields.to_a
  end

  # min chosen because it does not have a corresponding AggregateBy
  def test_create_group_by_many_fields
    group = nil
    assembly = mock_assembly do
      group = group_by 'offset', 'line' do
        min 'offset' => 'min_offset'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Min, assembly.tail_pipe.aggregator.class

    grouping_fields = group.key_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a

    assert_equal ['offset', 'line', 'min_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'min_offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_aggregate_by_many_fields
    group = nil
    assembly = mock_assembly do
      group = group_by 'offset', 'line' do
        average 'offset' => 'avg_offset'
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is an AverageFinal, not an Average
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingPipeAssembly::AverageBy::AverageFinal, assembly.tail_pipe.aggregator.class

    assert_equal ['offset', 'line', 'avg_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'avg_offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort
    group = nil
    assembly = mock_assembly do
      group = group_by 'offset', 'line', :sort_by => 'line' do
        count
      end
    end

    # :sort_by invalidates AggregateBy optimization
    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Count, assembly.tail_pipe.aggregator.class

    assert group.is_sorted
    assert !group.is_sort_reversed

    grouping_fields = group.key_selectors['test']
    sorting_fields = group.sorting_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort_reverse
    group = nil
    assembly = mock_assembly do
      group = group_by 'offset', 'line', :sort_by => 'line', :reverse => true do
        count
      end
    end

    # :sort_by invalidates AggregateBy optimization
    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Count, assembly.tail_pipe.aggregator.class

    assert group.is_sorted
    assert group.is_sort_reversed

    grouping_fields = group.key_selectors['test']
    sorting_fields = group.sorting_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_reverse
    group = nil
    assembly = mock_assembly do
      group = group_by 'offset', 'line', :reverse => true do
        count
      end
    end

    # :reverse invalidates AggregateBy optimization
    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Count, assembly.tail_pipe.aggregator.class

    assert !group.is_sorted
    assert group.is_sort_reversed

    grouping_fields = group.key_selectors['test']
    sorting_fields = group.sorting_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_nil sorting_fields

    assert_equal ['offset', 'line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count'], assembly.scope.grouping_fields.to_a
  end

  # first chosen because it does not have a corresponding AggregateBy
  def test_create_union
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => 'line' do
        first 'offset'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::First, assembly.tail_pipe.aggregator.class

    left_grouping_fields = group.key_selectors['test1']
    assert_equal ['line'], left_grouping_fields.to_a

    right_grouping_fields = group.key_selectors['test2']
    assert_equal ['line'], right_grouping_fields.to_a

    assert_equal ['line', 'offset'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'offset'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => 'offset' do
        first 'line'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::First, assembly.tail_pipe.aggregator.class

    left_grouping_fields = group.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = group.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      group = union 'test1', 'test2' do
        min 'offset' => 'min_offset'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Min, assembly.tail_pipe.aggregator.class

    left_grouping_fields = group.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = group.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'min_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'min_offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_aggregate_by
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => 'line' do
        sum 'offset'
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is a Sum
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Sum, assembly.tail_pipe.aggregator.class

    assert_equal ['line', 'offset'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'offset'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => 'offset' do
        sum :mapping => {'offset' => 'sum_offset'}, :type => :double
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is a Sum
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Sum, assembly.tail_pipe.aggregator.class

    assert_equal ['offset', 'sum_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'sum_offset'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      group = union 'test1', 'test2' do
        sum :mapping => {'offset' => 'sum_offset'}
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is a Sum
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Sum, assembly.tail_pipe.aggregator.class

    assert_equal ['offset', 'sum_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'sum_offset'], assembly.scope.grouping_fields.to_a
  end

  # max chosen because it does not have a corresponding AggregateBy
  def test_create_union_many_fields
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => ['offset', 'line'] do
        max 'offset' => 'max_offset'
      end
    end

    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Max, assembly.tail_pipe.aggregator.class

    left_grouping_fields = group.key_selectors['test1']
    assert_equal ['offset', 'line'], left_grouping_fields.to_a

    right_grouping_fields = group.key_selectors['test2']
    assert_equal ['offset', 'line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line', 'max_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'max_offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_aggregate_by_many_fields
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => ['offset', 'line'] do
        count
        average 'offset' => 'avg_offset'
      end
    end

    # GroupBy replaced by SubAssembly, the tail of which is an AverageFinal, not an Average
    assert_equal SubAssembly, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingPipeAssembly::AverageBy::AverageFinal, assembly.tail_pipe.aggregator.class

    assert_equal ['offset', 'line', 'count', 'avg_offset'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count', 'avg_offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_with_sort
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line' do
        count
      end
    end

    # :sort_by invalidates AggregateBy optimization
    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Count, assembly.tail_pipe.aggregator.class

    assert group.is_sorted
    assert !group.is_sort_reversed

    left_grouping_fields = group.key_selectors['test1']
    right_grouping_fields = group.key_selectors['test2']
    left_sorting_fields = group.sorting_selectors['test1']
    right_sorting_fields = group.sorting_selectors['test2']

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['line'], left_sorting_fields.to_a
    assert_equal ['line'], right_sorting_fields.to_a

    assert_equal ['offset', 'line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_with_sort_reverse
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line', :reverse => true do
        count
      end
    end

    # :sort_by invalidates AggregateBy optimization
    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Count, assembly.tail_pipe.aggregator.class

    assert group.is_sorted
    assert group.is_sort_reversed

    left_grouping_fields = group.key_selectors['test1']
    right_grouping_fields = group.key_selectors['test2']
    left_sorting_fields = group.sorting_selectors['test1']
    right_sorting_fields = group.sorting_selectors['test2']

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['line'], left_sorting_fields.to_a
    assert_equal ['line'], right_sorting_fields.to_a

    assert_equal ['offset', 'line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_reverse
    group = nil
    assembly = mock_branched_assembly do
      group = union 'test1', 'test2', :on => ['offset', 'line'], :reverse => true do
        count
      end
    end

    # :reverse invalidates AggregateBy optimization
    assert_equal Java::CascadingPipe::GroupBy, group.class
    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
    assert assembly.tail_pipe.aggregator?
    assert_equal Java::CascadingOperationAggregator::Count, assembly.tail_pipe.aggregator.class

    assert group.is_sorted # FIXME: Missing constructor in wip-255
    assert group.is_sort_reversed

    left_grouping_fields = group.key_selectors['test1']
    right_grouping_fields = group.key_selectors['test2']
    left_sorting_fields = group.sorting_selectors['test1']
    right_sorting_fields = group.sorting_selectors['test2']

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['offset', 'line'], left_sorting_fields.to_a # FIXME: Missing constructor in wip-255
    assert_equal ['offset', 'line'], right_sorting_fields.to_a # FIXME: Missing constructor in wip-255

    assert_equal ['offset', 'line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line', 'count'], assembly.scope.grouping_fields.to_a
  end
end
