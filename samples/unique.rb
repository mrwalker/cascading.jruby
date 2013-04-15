#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

# An example of using the Unique SubAssembly directly from a cascading.jruby job

cascade 'unique', :mode => :local do
  flow 'unique' do
    source 'input', tap('samples/data/data_group_by.tsv')

    assembly 'input' do
      split 'line', /\t/, ['id', 'city'], :output => ['id', 'city']

      branch 'unique' do
        sub_assembly Java::CascadingPipeAssembly::Unique.new(tail_pipe, fields('city'))
      end
    end

    sink 'unique', tap('output/unique', :sink_mode => :replace)
  end
end.complete
