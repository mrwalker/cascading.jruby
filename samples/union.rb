#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

cascade 'union', :mode => :local do
  flow 'union' do
    # You don't have to curl and cache inputs: tap can fetch via HTTP
    #source 'input', tap('http://www.census.gov/genealogy/names/dist.all.last')
    source 'input', tap('samples/data/genealogy/names/dist.all.last')

    assembly 'input' do
      split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id']

      branch 'branch1' do
        group_by 'score1', 'name' do
          count
        end
        rename 'score1' => 'score'
      end

      branch 'branch2' do
        group_by 'score2', 'name' do
          count
        end
        rename 'score2' => 'score'
      end
    end

    assembly 'union' do
      union 'branch1', 'branch2'
    end

    sink 'union', tap('output/union', :sink_mode => :replace)
  end
end.complete
