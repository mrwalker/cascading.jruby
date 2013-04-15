#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'splitter', :mode => :local do
  flow 'splitter' do
    source 'input', tap('samples/data/data2.txt')

    assembly 'input' do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      group_by 'score1' do
        count
      end
    end

    sink 'input', tap('output/splitter', :sink_mode => :replace)
  end
end.complete
