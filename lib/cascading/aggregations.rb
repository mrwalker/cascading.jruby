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
  # * Must follow a GroupBy or CoGroup (not a Join or Merge)
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

    def initialize(assembly, group, incoming_scopes)
      @assembly = assembly
      @tail_pipe = group
      @scope = Scope.outgoing_scope(tail_pipe, incoming_scopes)

      # AggregateBy optimization only applies to GroupBy
      @aggregate_bys = tail_pipe.is_group_by ? [] : nil
    end

    def debug_scope
      puts "Current scope of aggregations for '#{assembly.name}':\n  #{scope}\n----------\n"
    end

    def make_pipe(type, parameters)
      pipe = type.new(*parameters)

      # Enforce 1 Buffer or >= 1 Aggregator rule
      if tail_pipe.kind_of?(Java::CascadingPipe::Every)
        raise 'Buffer must be sole aggregation' if tail_pipe.buffer? || (tail_pipe.aggregator? && pipe.buffer?)
      end

      @tail_pipe = pipe
      @scope = Scope.outgoing_scope(tail_pipe, [scope])
    end
    private :make_pipe

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
    # Aggregators.
    def every(*args)
      options = args.extract_options!

      in_fields = fields(args)
      out_fields = fields(options[:output])
      operation = options[:aggregator] || options[:buffer]

      if options[:aggregate_by] && aggregate_bys
        aggregate_bys << options[:aggregate_by]
      else
        @aggregate_bys = nil
      end

      parameters = [tail_pipe, in_fields, operation, out_fields].compact
      make_pipe(Java::CascadingPipe::Every, parameters)
    end

    def assert_group(*args)
      options = args.extract_options!

      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      parameters = [tail_pipe, assertion_level, assertion]
      make_pipe(Java::CascadingPipe::Every, parameters)
    end

    def assert_group_size_equals(*args)
      options = args.extract_options!

      assertion = Java::CascadingOperationAssertion::AssertGroupSizeEquals.new(args[0])
      assert_group(assertion, options)
    end

    # Builds a series of every pipes for aggregation.
    #
    # Args can either be a list of fields to aggregate and an options hash or
    # a hash that maps input field name to output field name (similar to
    # insert) and an options hash.
    #
    # Options include:
    # * <tt>:ignore</tt> a Java Array of Objects (for min and max) or Tuples
    # (for first and last) of values for the aggregator to ignore
    # * <tt>function</tt> is a symbol that is the method to call to construct
    # the Cascading Aggregator.
    def composite_aggregator(args, function)
      field_map, options = extract_field_map(args)

      field_map.each do |in_field, out_field|
        agg = self.send(function, out_field, options)
        every(in_field, :aggregator => agg, :output => all_fields)
      end
      raise "Composite aggregator '#{function.to_s.gsub('_function', '')}' invoked on 0 fields" if field_map.empty?
    end

    def min(*args); composite_aggregator(args, :min_function); end
    def max(*args); composite_aggregator(args, :max_function); end
    def first(*args); composite_aggregator(args, :first_function); end
    def last(*args); composite_aggregator(args, :last_function); end

    # Counts elements of a group.  May optionally specify the name of the
    # output count field (defaults to 'count').
    def count(name = 'count')
      count_aggregator = Java::CascadingOperationAggregator::Count.new(fields(name))
      count_by = Java::CascadingPipeAssembly::CountBy.new(fields(name))
      every(last_grouping_fields, :aggregator => count_aggregator, :output => all_fields, :aggregate_by => count_by)
    end

    # Sums one or more fields.  Fields to be summed may either be provided as
    # the arguments to sum (in which case they will be aggregated into a field
    # of the same name in the given order), or via a hash using the :mapping
    # parameter (in which case they will be aggregated from the field named by
    # the key into the field named by the value after being sorted).  The type
    # of the output sum may be controlled with the :type parameter.
    def sum(*args)
      options = args.extract_options!
      type = JAVA_TYPE_MAP[options[:type]]

      mapping = options[:mapping] ? options[:mapping].sort : args.zip(args)
      mapping.each do |in_field, out_field|
        sum_aggregator = Java::CascadingOperationAggregator::Sum.new(*[fields(out_field), type].compact)
        # NOTE: SumBy requires a type in wip-286, unlike Sum (see Sum.java line 42 for default)
        sum_by = Java::CascadingPipeAssembly::SumBy.new(fields(in_field), fields(out_field), type || Java::double.java_class)
        every(in_field, :aggregator => sum_aggregator, :output => all_fields, :aggregate_by => sum_by)
      end
      raise "sum invoked on 0 fields (note :mapping must be provided to explicitly rename fields)" if mapping.empty?
    end

    # Averages one or more fields.  The contract of average is identical to
    # that of other composite aggregators, but it accepts no options.
    def average(*args)
      field_map, _ = extract_field_map(args)

      field_map.each do |in_field, out_field|
        average_aggregator = Java::CascadingOperationAggregator::Average.new(fields(out_field))
        average_by = Java::CascadingPipeAssembly::AverageBy.new(fields(in_field), fields(out_field))
        every(in_field, :aggregator => average_aggregator, :output => all_fields, :aggregate_by => average_by)
      end
      raise "average invoked on 0 fields" if field_map.empty?
    end

    private

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

    def aggregator_function(args, aggregator_klass)
      options = args.extract_options!
      ignore = options[:ignore]

      parameters = [Cascading.fields(args), ignore].compact
      aggregator_klass.new(*parameters)
    end

    def first_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::First)
    end

    def min_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Min)
    end

    def max_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Max)
    end

    def last_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Last)
    end
  end
end
