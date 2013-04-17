#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'scorenames', :mode => :local do
  flow 'scorenames' do
    # You don't have to curl and cache inputs: tap can fetch via HTTP
    #source 'input', tap('http://www.census.gov/genealogy/names/dist.all.last')
    source 'input', tap('samples/data/genealogy/names/dist.all.last')

    assembly 'input' do
      split 'line', /[.,]*\s+/, ['name', 'val1', 'val2', 'id']
      insert 'val3' => expr('val2:double < 40.0 ? val1:double : val2:double')
      project 'name', 'val3', 'id'
    end

    sink 'input', tap('output/scorenames', :sink_mode => :replace)
  end
end.complete
