#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'rename', :mode => :local do
  flow 'rename' do
    source 'input', tap('samples/data/data2.txt')

    assembly 'input' do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
      rename 'name' => 'new_name', 'score1' => 'new_score1', 'score2' => 'new_score2'
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
      puts "Final field names: #{scope.values_fields.to_a.inspect}"
    end

    sink 'input', tap('output/rename', :sink_mode => :replace)
  end
end.complete
