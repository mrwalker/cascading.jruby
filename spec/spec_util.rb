OUTPUT_DIR = 'output'
BUILD_DIR = 'build/spec'

module ScopeTests
  def check_scope(params = {})
    name_params = [params[:source]].compact
    scope = scope(*name_params)
    values_fields = params[:values_fields]
    grouping_fields = params[:grouping_fields] || values_fields

    debug = params[:debug]
    debug_scope(*name_params) if debug

    scope.values_fields.to_a.should == values_fields
    scope.grouping_fields.to_a.should == grouping_fields
  end
end

module Cascading
  class Flow; include ScopeTests; end
  class Assembly; include ScopeTests; end
  class Aggregations; include ScopeTests; end
end

def test_flow(&block)
  cascade = cascade 'test_app', :mode => :local do
    flow 'test', &block
  end
  cascade.complete
end

def test_assembly(params = {}, &block)
  branches = params[:branches] || []

  test_flow do
    source 'input', tap('spec/resource/test_input.txt', :scheme => text_line_scheme)

    # Default Fields defined by TextLineScheme
    check_scope :source => 'input', :values_fields => ['offset', 'line']

    assembly 'input', &block

    sink 'input', tap("#{OUTPUT_DIR}/out.txt", :sink_mode => :replace)

    # Branches must be sunk so that they (and their assertions) will be run
    branches.each do |branch|
      sink branch, tap("#{OUTPUT_DIR}/#{branch}_out.txt", :sink_mode => :replace)
    end
  end
end

def test_join_assembly(params = {}, &block)
  branches = params[:branches] || []
  post_join_block = params[:post_join_block]

  test_flow do
    source 'left', tap('spec/resource/join_input.txt', :scheme => text_line_scheme)
    source 'right', tap('spec/resource/join_input.txt', :scheme => text_line_scheme)

    # Default Fields defined by TextLineScheme
    check_scope :source => 'left', :values_fields => ['offset', 'line']
    check_scope :source => 'right', :values_fields => ['offset', 'line']

    assembly 'left' do
      check_scope :values_fields => ['offset', 'line']
      split 'line', /,/, ['x', 'y', 'z']
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z']
    end

    assembly 'right' do
      check_scope :values_fields => ['offset', 'line']
      split 'line', /,/, ['x', 'y', 'z']
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z']
    end

    assembly 'join' do
      # Empty scope because there is no 'join' source or assembly
      check_scope :values_fields => []

      left_join 'left', 'right', :on => ['x'], &block

      instance_eval &post_join_block if post_join_block
    end

    sink 'join', tap("#{OUTPUT_DIR}/join_out.txt", :sink_mode => :replace)

    # Branches must be sunk so that they (and their assertions) will be run
    branches.each do |branch|
      sink branch, tap("#{OUTPUT_DIR}/#{branch}_out.txt", :sink_mode => :replace)
    end
  end
end
