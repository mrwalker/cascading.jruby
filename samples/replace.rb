#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'replace', :mode => :local do
  flow 'replace' do
    source 'input', tap('samples/data/data2.txt')

    assembly 'input' do
      replace 'line', /[.,]*\s+/, 'tab_separated_line', "\t", :output => 'tab_separated_line'
    end

    sink 'input', tap('output/replace', :sink_mode => :replace)
  end
end.complete
