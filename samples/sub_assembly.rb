#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'sub_assembly', :mode => :local do
  flow 'sub_assembly' do
    source 'input', tap('samples/data/data2.txt')

    assembly 'input' do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
      sub_assembly Java::CascadingPipeAssembly::Discard.new(tail_pipe, fields('id'))
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(3)

      sub_assembly Java::CascadingPipeAssembly::Unique.new(tail_pipe, fields('name'))
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(3)

      sub_assembly Java::CascadingPipeAssembly::Retain.new(tail_pipe, fields(['score1', 'score2']))
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(2)

      sub_assembly Java::CascadingPipeAssembly::Rename.new(tail_pipe, fields(['score1', 'score2']), fields(['score_a', 'score_b']))
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(2)

      debug_scope
    end

    sink 'input', tap('output/sub_assembly', :sink_mode => :replace)
  end
end.complete
