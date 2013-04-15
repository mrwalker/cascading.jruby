#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'

c = cascade 'group_by', :mode => :local do
  flow 'group_by' do
    source 'input', tap('samples/data/data_group_by.tsv')

    assembly 'input' do
      split 'line', /\t/, ['id', 'city'], :output => ['id', 'city']

      branch 'group_by' do
        group_by 'city', :sort_by => 'city' do
          count
          sum 'id', :type => :int
        end
      end

      branch 'empty_group_by' do
        group_by 'city', :sort_by => 'city' do
        end
      end

      branch 'blockless_group_by' do
        group_by 'city', :sort_by => 'city'
      end

      branch 'aggregate_by' do
        group_by 'city' do
          count
          sum 'id', :type => :int
        end
      end

      # These compile into GroupBy unless we relax Aggregations#can_aggregate?
      # to allow empty Aggregations#aggregate_bys, which does not make sense
      #branch 'empty_aggregate_by' do
      #  group_by 'city' do
      #  end
      #end

      #branch 'blockless_aggregate_by' do
      #  group_by 'city'
      #end
    end

    sink 'group_by', tap('output/group_by', :sink_mode => :replace)
    sink 'empty_group_by', tap('output/empty_group_by', :sink_mode => :replace)
    sink 'blockless_group_by', tap('output/blockless_group_by', :sink_mode => :replace)
    sink 'aggregate_by', tap('output/aggregate_by', :sink_mode => :replace)
    #sink 'empty_aggregate_by', tap('output/empty_aggregate_by', :sink_mode => :replace)
    #sink 'blockless_aggregate_by', tap('output/blockless_aggregate_by', :sink_mode => :replace)
  end
end

# This sample can optionally draw itself if an output directory is provided
ARGV.empty? ? c.complete : c.draw(ARGV[0])
