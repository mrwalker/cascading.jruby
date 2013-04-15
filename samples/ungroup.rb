#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

# This sample exposes what the ungroup helper compiles into.  It also grabs the
# 'input' assembly's children and iterates over them to create sinks for each
# in a formulaic way.

flow 'ungroup', :mode => :local do
  source 'input', tap('samples/data/ungroup.tsv')

  a = assembly 'input' do
    split 'line', /\t/, ['key', 'val1', 'val2', 'val3'], :output => ['key', 'val1', 'val2', 'val3']

    branch 'ungroup_using_value_selectors' do
      #each all_fields, :function => Java::CascadingOperationFunction::UnGroup.new(fields(['new_key', 'val']), fields('key'), [fields('val1'), fields('val2'), fields('val3')].to_java(Java::CascadingTuple::Fields)), :output => ['new_key', 'val']
      ungroup :key => 'key', :value_selectors => ['val1', 'val2', 'val3'], :into => ['new_key', 'val'], :output => ['new_key', 'val']
    end

    branch 'ungroup_using_num_values' do
      #each all_fields, :function => Java::CascadingOperationFunction::UnGroup.new(fields(['new_key', 'val']), fields('key'), 1), :output => ['new_key', 'val']
      ungroup :key => 'key', :num_values => 1, :into => ['new_key', 'val'], :output => ['new_key', 'val']
    end

    # This pairs up the first and last two fields with no "key"
    branch 'ungroup_no_key' do
      #each all_fields, :function => Java::CascadingOperationFunction::UnGroup.new(fields(['left', 'right']), fields([]), 2), :output => ['left', 'right']
      ungroup :key => [], :num_values => 2, :into => ['left', 'right'], :output => ['left', 'right']
    end
  end

  a.children.map do |name, assembly|
    sink name, tap("output/#{name}", :sink_mode => :replace)
  end
end.complete
