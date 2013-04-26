require 'cascading/scope'
require 'cascading/ext/array'

module Cascading
  # Aggregations is the context available to you within the block of a group_by,
  # union, or join that allows you to apply Every pipes to the result of those
  # operations.  You may apply aggregators and buffers within this context
  # subject to several rules laid out by Cascading.
  #
  # Rules enforced by Aggregations:
  # * Contains either 1 Buffer or >= 1 Aggregator (explicitly checked)
  # * No GroupBys, CoGroups, Joins, or Merges (methods for these pipes do not exist on Aggregations)
  # * No Eaches (Aggregations#each does not exist)
  # * Aggregations may not branch (Aggregations#branch does not exist)
  #
  # Externally enforced rules:
  # * May be empty (in which case, Aggregations is not instantiated)
  # * Must follow a GroupBy or CoGroup (not a HashJoin or Merge)
  #
  # Optimizations:
  # * If the leading Group is a GroupBy and all subsequent Everies are Aggregators that have a corresponding AggregateBy, Aggregations can replace the GroupBy/Aggregator pipe with a single composite AggregateBy
  #
  # Aggregator and buffer DSL standard optional parameter names:
  # [input] c.p.Every argument selector
  # [into] c.o.Operation field declaration
  # [output] c.p.Every output selector
  class Aggregations
    attr_reader :assembly, :tail_pipe, :scope, :aggregate_bys

    # Do not use this constructor directly; instead, pass a block containing
    # the desired aggregations to a group_by, union, or join and it will be
    # instantiated for you.
    #
    # Builds the context in which a sequence of Every aggregations may be
    # evaluated in the given assembly appended to the given group pipe and with
    # the given incoming_scopes.
    def initialize(assembly, group, incoming_scopes)
      @assembly = assembly
      @tail_pipe = group
      @scope = Scope.outgoing_scope(tail_pipe, incoming_scopes)

      # AggregateBy optimization only applies to GroupBy
      @aggregate_bys = tail_pipe.is_group_by ? [] : nil
    end

    # Prints information about the scope of these Aggregations at the point at
    # which it is called.  This allows you to trace the propagation of field
    # names through your job and is handy for debugging.  See Scope for
    # details.
    def debug_scope
      puts "Current scope of aggregations for '#{assembly.name}':\n  #{scope}\n----------\n"
    end

    # We can replace these aggregations with the corresponding composite
    # AggregateBy if the leading Group was a GroupBy and all subsequent
    # Aggregators had a corresponding AggregateBy (which we've encoded in the
    # list of aggregate_bys being a non-empty array).
    def can_aggregate_by?
      !aggregate_bys.nil? && !aggregate_bys.empty?
    end

    # "Fix" out values fields after a sequence of Everies.  This is a field
    # name metadata fix which is why the Identity is not planned into the
    # resulting Cascading pipe.  Without it, all values fields would propagate
    # through non-empty aggregations, which doesn't match Cascading's planner's
    # behavior.
    def finalize
      discard_each = Java::CascadingPipe::Each.new(tail_pipe, all_fields, Java::CascadingOperation::Identity.new)
      @scope = Scope.outgoing_scope(discard_each, [scope])
    end

    # Builds an every pipe and adds it to the current list of aggregations.
    # Note that this list may be either exactly 1 Buffer or any number of
    # Aggregators.  Exactly one of :aggregator or :buffer must be specified and
    # :aggregator may be accompanied by a corresponding :aggregate_by.
    #
    # The named options are:
    # [aggregator] A Cascading Aggregator, mutually exclusive with :buffer.
    # [aggregate_by] A Cascading AggregateBy that corresponds to the given
    #                :aggregator.  Only makes sense with the :aggregator option
    #                and does not exist for all Aggregators.  Providing nothing
    #                or nil will cause all Aggregations to operate as normal,
    #                without being compiled into a composite AggregateBy.
    # [buffer] A Cascading Buffer, mutually exclusive with :aggregator.
    # [output] c.p.Every output selector.
    #
    # Example:
    #    every 'field1', 'field2', :aggregator => sum_aggregator, :aggregate_by => sum_by, :output => all_fields
    #    every fields(input_fields), :buffer => Java::SomePackage::SomeBuffer.new, :output => all_fields
    def every(*args_with_options)
      options, in_fields = args_with_options.extract_options!, fields(args_with_options)
      out_fields = fields(options[:output])
      operation = options[:aggregator] || options[:buffer]
      raise 'every requires either :aggregator or :buffer' unless operation

      if options[:aggregate_by] && aggregate_bys
        aggregate_bys << options[:aggregate_by]
      else
        @aggregate_bys = nil
      end

      parameters = [tail_pipe, in_fields, operation, out_fields].compact
      every = make_pipe(Java::CascadingPipe::Every, parameters)
      raise ':aggregator specified but c.o.Buffer provided' if options[:aggregator] && every.is_buffer
      raise ':buffer specified but c.o.Aggregator provided' if options[:buffer] && every.is_aggregator

      every
    end

    # Builds an every assertion pipe given a c.o.a.Assertion and adds it to the
    # current list of aggregations.  Note this breaks a chain of AggregateBys.
    #
    # The named options are:
    # [level] The assertion level; defaults to strict.
    def assert_group(assertion, options = {})
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      parameters = [tail_pipe, assertion_level, assertion]
      make_pipe(Java::CascadingPipe::Every, parameters)
    end

    # Builds a pipe that asserts the size of the current group is the specified
    # size for all groups.
    def assert_group_size_equals(size, options = {})
      assertion = Java::CascadingOperationAssertion::AssertGroupSizeEquals.new(size)
      assert_group(assertion, options)
    end

    # Computes the minima of the specified fields within each group.  Fields
    # may be a list or a map for renaming.  Note that fields are sorted by
    # input name when a map is provided.
    #
    # The named options are:
    # [ignore] Java Array of Objects of values to be ignored.
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       insert 'const' => 1
    #       group_by 'const' do
    #         min 'field1', 'field2'
    #         min 'field3' => 'fieldA', 'field4' => 'fieldB'
    #       end
    #       discard 'const'
    #     end
    def min(*args_with_options)
      composite_aggregator(args_with_options, Java::CascadingOperationAggregator::Min)
    end

    # Computes the maxima of the specified fields within each group.  Fields
    # may be a list or a map for renaming.  Note that fields are sorted by
    # input name when a map is provided.
    #
    # The named options are:
    # [ignore] Java Array of Objects of values to be ignored.
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       insert 'const' => 1
    #       group_by 'const' do
    #         max 'field1', 'field2'
    #         max 'field3' => 'fieldA', 'field4' => 'fieldB'
    #       end
    #       discard 'const'
    #     end
    def max(*args_with_options)
      composite_aggregator(args_with_options, Java::CascadingOperationAggregator::Max)
    end

    # Returns the first value within each group for the specified fields.
    # Fields may be a list or a map for renaming.  Note that fields are sorted
    # by input name when a map is provided.
    #
    # The named options are:
    # [ignore] Java Array of Tuples which should be ignored
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       group_by 'key1', 'key2' do
    #         first 'field1', 'field2'
    #         first 'field3' => 'fieldA', 'field4' => 'fieldB'
    #       end
    #     end
    def first(*args_with_options)
      composite_aggregator(args_with_options, Java::CascadingOperationAggregator::First)
    end

    # Returns the last value within each group for the specified fields.
    # Fields may be a list or a map for renaming.  Note that fields are sorted
    # by input name when a map is provided.
    #
    # The named options are:
    # [ignore] Java Array of Tuples which should be ignored
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       group_by 'key1', 'key2' do
    #         last 'field1', 'field2'
    #         last 'field3' => 'fieldA', 'field4' => 'fieldB'
    #       end
    #     end
    def last(*args_with_options)
      composite_aggregator(args_with_options, Java::CascadingOperationAggregator::Last)
    end

    # Counts elements of each group.  May optionally specify the name of the
    # output count field, which defaults to 'count'.
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       group_by 'key1', 'key2' do
    #         count
    #         count 'key1_key2_count'
    #       end
    #     end
    def count(name = 'count')
      count_aggregator = Java::CascadingOperationAggregator::Count.new(fields(name))
      count_by = Java::CascadingPipeAssembly::CountBy.new(fields(name))
      every(last_grouping_fields, :aggregator => count_aggregator, :output => all_fields, :aggregate_by => count_by)
    end

    # Sums the specified fields within each group.  Fields may be a list or
    # provided through the :mapping option for renaming.  Note that fields are
    # sorted by name when a map is provided.
    #
    # The named options are:
    # [mapping] Map of input to output field names if renaming is desired.
    #           Results in output fields sorted by input field.
    # [type] Controls the type of the output, specified using values from the
    #        Cascading::JAVA_TYPE_MAP as in Janino expressions (:double, :long, etc.)
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       group_by 'key1', 'key2' do
    #         sum 'field1', 'field2', :type => :long
    #         sum :mapping => { 'field3' => 'fieldA', 'field4' => 'fieldB' }, :type => :double
    #       end
    #     end
    def sum(*args_with_options)
      options, in_fields = args_with_options.extract_options!, args_with_options
      type = JAVA_TYPE_MAP[options[:type]]

      mapping = options[:mapping] ? options[:mapping].sort : in_fields.zip(in_fields)
      mapping.each do |in_field, out_field|
        sum_aggregator = Java::CascadingOperationAggregator::Sum.new(*[fields(out_field), type].compact)
        # NOTE: SumBy requires a type in wip-286, unlike Sum (see Sum.java line 42 for default)
        sum_by = Java::CascadingPipeAssembly::SumBy.new(fields(in_field), fields(out_field), type || Java::double.java_class)
        every(in_field, :aggregator => sum_aggregator, :output => all_fields, :aggregate_by => sum_by)
      end
      raise "sum invoked on 0 fields (note :mapping must be provided to explicitly rename fields)" if mapping.empty?
    end

    # Averages the specified fields within each group.  Fields may be a list or
    # a map for renaming.  Note that fields are sorted by input name when a map
    # is provided.
    #
    # Examples:
    #     assembly 'aggregate' do
    #       ...
    #       insert 'const' => 1
    #       group_by 'const' do
    #         max 'field1', 'field2'
    #         max 'field3' => 'fieldA', 'field4' => 'fieldB'
    #       end
    #       discard 'const'
    #     end
    def average(*fields_or_field_map)
      field_map, _ = extract_field_map(fields_or_field_map)

      field_map.each do |in_field, out_field|
        average_aggregator = Java::CascadingOperationAggregator::Average.new(fields(out_field))
        average_by = Java::CascadingPipeAssembly::AverageBy.new(fields(in_field), fields(out_field))
        every(in_field, :aggregator => average_aggregator, :output => all_fields, :aggregate_by => average_by)
      end
      raise "average invoked on 0 fields" if field_map.empty?
    end

    private

    def make_pipe(type, parameters)
      pipe = type.new(*parameters)

      # Enforce 1 Buffer or >= 1 Aggregator rule
      if tail_pipe.kind_of?(Java::CascadingPipe::Every)
        raise 'Buffer must be sole aggregation' if tail_pipe.buffer? || (tail_pipe.aggregator? && pipe.buffer?)
      end

      @tail_pipe = pipe
      @scope = Scope.outgoing_scope(tail_pipe, [scope])

      tail_pipe
    end

    # Builds a series of every pipes for aggregation.
    #
    # Args can either be a list of fields to aggregate and an options hash or
    # a hash that maps input field name to output field name (similar to
    # insert) and an options hash.
    #
    # The named options are:
    # [ignore] Java Array of Objects (for min and max) or Tuples (for first and
    #          last) of values for the aggregator to ignore.
    def composite_aggregator(args, aggregator)
      field_map, options = extract_field_map(args)

      field_map.each do |in_field, out_field|
        every(
          in_field,
          :aggregator => aggregator.new(*[fields(out_field), options[:ignore]].compact),
          :output => all_fields
        )
      end
      raise "Composite aggregator '#{aggregator}' invoked on 0 fields" if field_map.empty?
    end

    # Extracts a field mapping, input field => output field, by accepting a
    # hash in the first argument.  If no hash is provided, then maps arguments
    # onto themselves which names outputs the same as inputs.  Additionally
    # extracts options from args.
    def extract_field_map(args)
      if !args.empty? && args.first.kind_of?(Hash)
        field_map = args.shift.sort
        options = args.extract_options!
      else
        options = args.extract_options!
        field_map = args.zip(args)
      end
      [field_map, options]
    end
  end
end
