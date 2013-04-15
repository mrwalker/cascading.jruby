require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

context Cascading::Scope do
  it 'should match Cascading fields names from source tap scheme' do
    test_assembly do
      # Pass that uses our scope instead of all_fields
      each scope.values_fields, :function => Java::CascadingOperation::Identity.new
      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should pick up names from source tap scheme' do
    test_assembly do
      pass

      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should propagate names through Each' do
    test_assembly do
      check_scope :values_fields => ['offset', 'line']
      assert_size_equals 2

      split 'line', /,/, ['x', 'y']
      check_scope :values_fields => ['offset', 'line', 'x', 'y']
      assert_size_equals 4
    end
  end

  it 'should allow field filtration at Each' do
    test_assembly do
      check_scope :values_fields => ['offset', 'line']
      assert_size_equals 2

      split 'line', /,/, ['x', 'y'], :output => ['x', 'y']
      check_scope :values_fields => ['x', 'y']
      assert_size_equals 2
    end
  end

  it 'should propagate names through CoGroup' do
    test_join_assembly do
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_']
    end
  end

  it 'should propagate names through CoGroup with no Aggregations' do
    post_join_block = lambda do |assembly|
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_']
    end

    test_join_assembly(:post_join_block => post_join_block)
  end

  it 'should pass grouping fields to Every' do
    test_join_assembly do
      sum :mapping => {'x' => 'x_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_', 'x_sum']
      assert_group_size_equals 1
    end
  end

  it 'should pass grouping fields through chained Every' do
    test_join_assembly do
      sum :mapping => {'x' => 'x_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_', 'x_sum']
      assert_group_size_equals 1

      sum :mapping => {'y' => 'y_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_', 'x_sum', 'y_sum']
      assert_group_size_equals 1
    end
  end

  it 'should propagate names through Every' do
    post_join_block = lambda do |assembly|
      check_scope :values_fields => ['x', 'x_', 'x_sum', 'y_sum']
      assert_size_equals 4
    end

    test_join_assembly :post_join_block => post_join_block do
      sum :mapping => {'x' => 'x_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_', 'x_sum']
      assert_group_size_equals 1

      sum :mapping => {'y' => 'y_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_', 'x_sum', 'y_sum']
      assert_group_size_equals 1
    end
  end

  it 'should pass values fields to Each immediately following CoGroup and remove grouping fields' do
    post_join_block = lambda do |assembly|
      assert_size_equals 10
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_']
    end
    test_join_assembly(:post_join_block => post_join_block)
  end

  it 'should fail to pass grouping fields to Every immediately following Each' do
    post_join_block = lambda do |assembly|
      pass
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_']
      sum :mapping => {'x' => 'x_sum'}, :type => :int
    end

    lambda do # Composition fails
      test_join_assembly(:post_join_block => post_join_block)
    # sum doesn't exist outside of Aggregations (where block of join is
    # evaluated)
    end.should raise_error NoMethodError
  end

  it 'should propagate values fields and field names into branch' do
    post_join_block = lambda do |assembly|
      branch 'data_tuple' do
        check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
          :grouping_fields => ['x', 'x_']
        assert_size_equals 10
      end
    end

    test_join_assembly(:branches => ['data_tuple'], :post_join_block => post_join_block)
  end

  it 'should propagate names through GroupBy' do
    test_assembly do
      group_by 'line' do
        count
      end
      check_scope :values_fields => ['line', 'count']
    end
  end

  it 'should propagate names through blockless GroupBy' do
    test_assembly do
      group_by 'line'
      check_scope :values_fields => ['offset', 'line'], :grouping_fields => ['line']
    end
  end
end
