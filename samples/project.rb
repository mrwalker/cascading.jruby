#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

# History: "project" (verb) used to be known as "restrict"

require 'cascading'

cascade 'project', :mode => :local do
  flow 'project' do
    source 'input', tap('samples/data/data2.txt')

    assembly 'input' do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
      project 'name', 'score1', 'score2'
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(3)
      project 'name', 'score2'
      assert Java::CascadingOperationAssertion::AssertSizeEquals.new(2)
    end

    sink 'input', tap('output/project', :sink_mode => :replace)
  end
end.complete
