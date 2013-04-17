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
      ungroup 'key', ['new_key', 'val'], :value_selectors => ['val1', 'val2', 'val3'], :output => ['new_key', 'val']
    end

    branch 'ungroup_using_num_values' do
      ungroup 'key', ['new_key', 'val'], :num_values => 1, :output => ['new_key', 'val']
    end

    # This pairs up the first and last two fields with no "key"
    branch 'ungroup_no_key' do
      ungroup [], ['left', 'right'], :num_values => 2, :output => ['left', 'right']
    end
  end

  a.children.map do |name, assembly|
    sink name, tap("output/#{name}", :sink_mode => :replace)
  end
end.complete
