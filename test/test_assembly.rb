require 'test/unit'
require 'cascading'
require 'test/mock_assemblies'

class TC_Assembly < Test::Unit::TestCase
  include MockAssemblies

  def test_create_assembly_simple
    assembly = nil
    flow 'test_create_assembly_simple' do
      assembly = assembly 'assembly1' do
        # Empty assembly
      end
    end

    assert_not_nil assembly
    assert_equal assembly.name, 'assembly1'
    assert_equal 0, assembly.children.size

    pipe = assembly.tail_pipe
    assert_equal Java::CascadingPipe::Pipe, pipe.class
  end

  def test_each_identity
    assembly = mock_assembly do
      each 'offset', :function => Java::CascadingOperation::Identity.new
    end

    flow = assembly.parent
    assert_not_nil flow
    assert_not_nil flow.find_child('test')
    assert_equal assembly, flow.find_child('test')
  end

  def test_create_each
    # You can apply an Each to 0 fields
    assembly = mock_assembly do
      each(:function => Java::CascadingOperation::Identity.new)
    end
    assert_equal Java::CascadingPipe::Each, assembly.tail_pipe.class

    # In which case, it has empty argument and output selectors
    assert_equal 0, assembly.tail_pipe.argument_selector.size
    assert_equal 0, assembly.tail_pipe.output_selector.size

    assembly = mock_assembly do
      each 'offset', :output => 'offset_copy', :function => Java::CascadingOperation::Identity.new(fields('offset_copy'))
    end
    pipe = assembly.tail_pipe

    assert_equal Java::CascadingPipe::Each, pipe.class

    assert_equal ['offset'], pipe.argument_selector.to_a
    assert_equal ['offset_copy'], pipe.output_selector.to_a
  end

  def test_every_cannot_follow_tap
    # Assembly#count is no longer defined; instead, it has moved to
    # Aggregations#count
    ex = assert_raise NameError do
      assembly = mock_assembly do
        count
      end
      pipe = assembly.tail_pipe
      assert_equal Java::CascadingPipe::Every, pipe.class
    end
    assert_match /^undefined local variable or method `count' for #<Cascading::Assembly:.*>$/, ex.message
  end

  def test_create_every
      assembly = mock_assembly do
        group_by 'line' do
          count_aggregator = Java::CascadingOperationAggregator::Count.new(fields('count'))
          every 'line', :aggregator => count_aggregator, :output => 'count'
        end
      end
      assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class
      assert_equal ['line'], assembly.tail_pipe.argument_selector.to_a
      assert_equal ['count'], assembly.tail_pipe.output_selector.to_a

      assembly = mock_assembly do
        group_by 'line' do
          count
        end
      end
      assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class

      # NOTE: this is not valid when we optimize using CountBy
      #assert_equal last_grouping_fields, assembly.tail_pipe.argument_selector
      assert_equal fields('count'), assembly.tail_pipe.argument_selector

      assert_equal all_fields, assembly.tail_pipe.output_selector
  end

  def test_create_group_by
    assembly = mock_assembly do
      group_by 'line'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['line'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['line'], assembly.scope.grouping_fields.to_a

    assembly = mock_assembly do
      group_by 'offset'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['offset'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_many_fields
    assembly = mock_assembly do
      group_by 'offset', 'line'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    assert_equal ['offset', 'line'], grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort
    assembly = mock_assembly do
      group_by 'offset', 'line', :sort_by => 'line'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert assembly.tail_pipe.is_sorted
    assert !assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_with_sort_reverse
    assembly = mock_assembly do
      group_by 'offset', 'line', :sort_by => 'line', :reverse => true
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_equal ['line'], sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_group_by_reverse
    assembly = mock_assembly do
      group_by 'offset', 'line', :reverse => true
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    grouping_fields = assembly.tail_pipe.key_selectors['test']
    sorting_fields = assembly.tail_pipe.sorting_selectors['test']

    assert !assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], grouping_fields.to_a
    assert_nil sorting_fields

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'line'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['line'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['line'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => 'offset'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset'], assembly.scope.grouping_fields.to_a

    assembly = mock_branched_assembly do
      union 'test1', 'test2'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_many_fields
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line']
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['offset', 'line'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['offset', 'line'], right_grouping_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_with_sort
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line'
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    left_sorting_fields = assembly.tail_pipe.sorting_selectors['test1']
    right_sorting_fields = assembly.tail_pipe.sorting_selectors['test2']

    assert assembly.tail_pipe.is_sorted
    assert !assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['line'], left_sorting_fields.to_a
    assert_equal ['line'], right_sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_with_sort_reverse
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :sort_by => 'line', :reverse => true
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    left_sorting_fields = assembly.tail_pipe.sorting_selectors['test1']
    right_sorting_fields = assembly.tail_pipe.sorting_selectors['test2']

    assert assembly.tail_pipe.is_sorted
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['line'], left_sorting_fields.to_a
    assert_equal ['line'], right_sorting_fields.to_a

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_create_union_reverse
    assembly = mock_branched_assembly do
      union 'test1', 'test2', :on => ['offset', 'line'], :reverse => true
    end

    assert_equal Java::CascadingPipe::GroupBy, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    left_sorting_fields = assembly.tail_pipe.sorting_selectors['test1']
    right_sorting_fields = assembly.tail_pipe.sorting_selectors['test2']

    assert assembly.tail_pipe.is_sorted # FIXME: Missing constructor in wip-255
    assert assembly.tail_pipe.is_sort_reversed

    assert_equal ['offset', 'line'], left_grouping_fields.to_a
    assert_equal ['offset', 'line'], right_grouping_fields.to_a
    assert_equal ['offset', 'line'], left_sorting_fields.to_a # FIXME: Missing constructor in wip-255
    assert_equal ['offset', 'line'], right_sorting_fields.to_a # FIXME: Missing constructor in wip-255

    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_union_undefined_inputs
    ex = assert_raise RuntimeError do
      flow 'test_union_undefined_inputs' do
        source 'data1', tap('test/data/data1.txt')

        assembly 'data1' do
          pass
        end

        assembly 'union' do
          union 'doesnotexist', 'data1'
        end

        sink 'union', tap('output/test_union_undefined_inputs')
      end
    end
    assert_equal "Could not find assembly 'doesnotexist' from 'union'", ex.message
  end

  def test_create_join
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'name'
    end

    assert_equal Java::CascadingPipe::CoGroup, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    assert_equal ['name', 'name_'], assembly.scope.grouping_fields.to_a

    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'id'
    end

    assert_equal Java::CascadingPipe::CoGroup, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['id'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['id'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    assert_equal ['id', 'id_'], assembly.scope.grouping_fields.to_a
  end

  def test_create_join_many_fields
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => ['name', 'id']
    end

    assert_equal Java::CascadingPipe::CoGroup, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name', 'id'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name', 'id'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    assert_equal ['name', 'id', 'name_', 'id_'], assembly.scope.grouping_fields.to_a
  end

  def test_create_join_with_declared_fields
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'name', :declared_fields => ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    end

    assert_equal Java::CascadingPipe::CoGroup, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name'], right_grouping_fields.to_a

    assert_equal ['a', 'b', 'c', 'd', 'e', 'f', 'g'], assembly.scope.values_fields.to_a
    assert_equal ['name', 'name_'], assembly.scope.grouping_fields.to_a
  end

  def test_join_with_block
    assembly = mock_two_input_assembly do
      join 'test1', 'test2', :on => 'name' do
        count
      end
    end

    assert_equal Java::CascadingPipe::Every, assembly.tail_pipe.class

    assert_equal ['name', 'name_', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['name', 'name_', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_join_undefined_inputs
    [:join, :hash_join].each do |join|
      ex = assert_raise RuntimeError do
        flow 'test_join_undefined_inputs' do
          source 'data1', tap('test/data/data1.txt')

          assembly 'data1' do
            pass
          end

          assembly 'join' do
            send(join, 'doesnotexist', 'data1', :on => 'name')
          end

          sink 'join', tap('output/test_join_undefined_inputs')
        end
      end
      assert_equal "Could not find assembly 'doesnotexist' from 'join'", ex.message
    end
  end

  def test_join_without_on
    [:join, :hash_join].each do |join|
      ex = assert_raise RuntimeError do
        mock_two_input_assembly do
          send(join, 'test1', 'test2')
        end
      end
      assert_equal 'join requires :on parameter', ex.message
    end
  end

  def test_join_invalid_on
    [:join, :hash_join].each do |join|
      ex = assert_raise RuntimeError do
        mock_two_input_assembly do
          send(join, 'test1', 'test2', :on => 1)
        end
      end
      assert_equal "Unsupported data type for :on in join: 'Fixnum'", ex.message
    end
  end

  def test_join_empty_on
    [:join, :hash_join].each do |join|
      ex = assert_raise RuntimeError do
        mock_two_input_assembly do
          send(join, 'test1', 'test2', :on => [])
        end
      end
      assert_equal "join requires non-empty :on parameter", ex.message

      ex = assert_raise RuntimeError do
        mock_two_input_assembly do
          send(join, 'test1', 'test2', :on => {})
        end
      end
      assert_equal "join requires non-empty :on parameter", ex.message
    end
  end

  def test_create_hash_join
    assembly = mock_two_input_assembly do
      hash_join 'test1', 'test2', :on => 'name'
    end

    assert_equal Java::CascadingPipe::HashJoin, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    # NOTE: Why does HashJoin produce different grouping fields than CoGroup?
    assert_equal ['name'], assembly.scope.grouping_fields.to_a

    assembly = mock_two_input_assembly do
      hash_join 'test1', 'test2', :on => 'id'
    end

    assert_equal Java::CascadingPipe::HashJoin, assembly.tail_pipe.class
    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['id'], left_grouping_fields.to_a
    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['id'], right_grouping_fields.to_a
    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    # NOTE: Why does HashJoin produce different grouping fields than CoGroup?
    assert_equal ['id'], assembly.scope.grouping_fields.to_a
  end

  def create_hash_join_many_fields
        assembly = mock_two_input_assembly do
      hash_join 'test1', 'test2', :on => ['name', 'id']
    end

    assert_equal Java::CascadingPipe::HashJoin, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name', 'id'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name', 'id'], right_grouping_fields.to_a

    assert_equal ['name', 'score1', 'score2', 'id', 'name_', 'id_', 'town'], assembly.scope.values_fields.to_a
    # NOTE: Why does HashJoin produce different grouping fields than CoGroup?
    assert_equal ['name', 'id'], assembly.scope.grouping_fields.to_a
  end

  def create_hash_join_with_declared_fields
    assembly = mock_two_input_assembly do
      hash_join 'test1', 'test2', :on => 'name', :declared_fields => ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    end

    assert_equal Java::CascadingPipe::HashJoin, assembly.tail_pipe.class

    left_grouping_fields = assembly.tail_pipe.key_selectors['test1']
    assert_equal ['name'], left_grouping_fields.to_a

    right_grouping_fields = assembly.tail_pipe.key_selectors['test2']
    assert_equal ['name'], right_grouping_fields.to_a

    assert_equal ['a', 'b', 'c', 'd', 'e', 'f', 'g'], assembly.scope.values_fields.to_a
    # NOTE: Why does HashJoin produce different grouping fields than CoGroup?
    assert_equal ['name'], assembly.scope.grouping_fields.to_a
  end

  def test_hash_join_with_block
    ex = assert_raise ArgumentError do
      mock_two_input_assembly do
        hash_join 'test1', 'test2', :on => 'name' do
          count
        end
      end
    end
    assert_equal "HashJoin doesn't support aggregations so the block provided to hash_join will be ignored", ex.message
  end

  def test_branch_unique
    assembly = mock_assembly do
      branch 'branch1' do
      end
    end

    assert_equal 1, assembly.children.size
  end

  def test_branch_empty
    assembly = mock_assembly do
      branch 'branch1' do
      end

      branch 'branch2' do
        branch 'branch3' do
        end
      end
    end

    assert_equal 2, assembly.children.size
    assert_equal 0, assembly.children['branch1'].children.size
    assert_equal 1, assembly.children['branch2'].children.size
  end

  def test_branch_single
    assembly = mock_assembly do
      branch 'branch1' do
        branch 'branch2' do
          each 'line', :function => Java::CascadingOperation::Identity.new
        end
      end
    end

    assert_equal 1, assembly.children.size
    assert_equal 1, assembly.children['branch1'].children.size
    assert_equal 0, assembly.children['branch1'].children['branch2'].children.size
  end

  def test_sub_assembly
    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Discard.new(tail_pipe, fields('offset'))
    end
    assert_equal ['line'], assembly.scope.values_fields.to_a

    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Retain.new(tail_pipe, fields('offset'))
    end
    assert_equal ['offset'], assembly.scope.values_fields.to_a

    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Rename.new(tail_pipe, fields(['offset', 'line']), fields(['byte', 'line']))
    end
    assert_equal ['byte', 'line'], assembly.scope.values_fields.to_a

    assembly = mock_assembly do
      sub_assembly Java::CascadingPipeAssembly::Unique.new(tail_pipe, fields('line'))
    end
    assert_equal ['offset', 'line'], assembly.scope.values_fields.to_a
    assert_equal ['offset', 'line'], assembly.scope.grouping_fields.to_a
  end

  def test_count_by_sub_assembly
    assembly = mock_branched_assembly do
      pipes, _ = populate_incoming_scopes(['test1', 'test2'])

      aggregate_by = Java::CascadingPipeAssembly::AggregateBy.new(
        name,
        pipes.to_java(Java::CascadingPipe::Pipe),
        fields('line'),
        [Java::CascadingPipeAssembly::CountBy.new(fields('count'))].to_java(Java::CascadingPipeAssembly::AggregateBy)
      )

      sub_assembly aggregate_by, pipes, @incoming_scopes
    end
    assert_equal ['line', 'count'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'count'], assembly.scope.grouping_fields.to_a
  end

  def test_average_by_sub_assembly
    assembly = mock_assembly do
      aggregate_by = Java::CascadingPipeAssembly::AggregateBy.new(
        name,
        [tail_pipe].to_java(Java::CascadingPipe::Pipe),
        fields('line'),
        [Java::CascadingPipeAssembly::AverageBy.new(fields('offset'), fields('average'))].to_java(Java::CascadingPipeAssembly::AggregateBy)
      )

      sub_assembly aggregate_by
    end
    assert_equal ['line', 'average'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'average'], assembly.scope.grouping_fields.to_a
  end

  def test_sum_by_sub_assembly
    assembly = mock_assembly do
      aggregate_by = Java::CascadingPipeAssembly::AggregateBy.new(
        name,
        [tail_pipe].to_java(Java::CascadingPipe::Pipe),
        fields('line'),
        [Java::CascadingPipeAssembly::SumBy.new(fields('offset'), fields('sum'), Java::double.java_class)].to_java(Java::CascadingPipeAssembly::AggregateBy)
      )

      sub_assembly aggregate_by
    end
    assert_equal ['line', 'sum'], assembly.scope.values_fields.to_a
    assert_equal ['line', 'sum'], assembly.scope.grouping_fields.to_a
  end

  def test_where
    assembly = mock_assembly do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      where 'score1:double < score2:double'
    end
    assert_equal Java::CascadingPipe::Each, assembly.tail_pipe.class
    assert_equal Java::CascadingOperationExpression::ExpressionFilter, assembly.tail_pipe.operation.class
  end

  def test_where_with_import
    assembly = mock_assembly do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      names = ['SMITH', 'JONES', 'BROWN']
      where "import java.util.Arrays;\nArrays.asList(new String[] { \"#{names.join('", "')}\" }).contains(name:string)"
    end
    assert_equal Java::CascadingPipe::Each, assembly.tail_pipe.class
    assert_equal Java::CascadingOperationExpression::ExpressionFilter, assembly.tail_pipe.operation.class
  end

  def test_rename
    assembly = mock_assembly do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      rename 'score2' => 'new_score2', 'score1' => 'new_score1', 'name' => 'new_name'
    end
    # Original order preserved
    assert_equal ['new_name', 'new_score1', 'new_score2', 'id'], assembly.scope.values_fields.to_a
  end

  def test_copy
    assembly = mock_assembly do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      copy 'score2' => 'new_score2', 'id' => 'new_id', 'name' => 'new_name'
    end
    # Original order preserved in copied fields
    assert_equal ['name', 'score1', 'score2', 'id', 'new_name', 'new_score2', 'new_id'], assembly.scope.values_fields.to_a
  end

  def test_smoke_test_describe
    cascade 'smoke' do
      flow 'smoke' do
        source 'input', tap('test/data/data1.txt')
        assembly 'input' do
          puts "Describe at assembly start: '#{describe}'"
          group_by 'line' do
            count
            sum 'offset', :type => :long
            puts "Describe at group_by end (falls out to top-level Cascading::describe): '#{describe}'"
          end
          puts "Describe at assembly end: '#{describe}'"
        end
        sink 'input', tap('output/test_smoke_test_describe')
      end
    end
  end

  def test_smoke_test_debug_scope
    cascade 'smoke' do
      flow 'smoke' do
        source 'input', tap('test/data/data1.txt')
        assembly 'input' do
          debug_scope
          group_by 'line' do
            count
            sum 'offset', :type => :long
            debug_scope
          end
          debug_scope
        end
        sink 'input', tap('output/test_smoke_test_debug_scope')
      end
    end
  end
end
