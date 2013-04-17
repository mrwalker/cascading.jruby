require 'cascading/base'
require 'cascading/operations'
require 'cascading/identity_operations'
require 'cascading/regex_operations'
require 'cascading/text_operations'
require 'cascading/aggregations'
require 'cascading/sub_assembly'
require 'cascading/ext/array'

module Cascading
  # An Assembly is a sequence of Cascading pipes (Each, GroupBy, CoGroup,
  # Every, and SubAssembly).  This class will serve as your primary mechanism
  # for doing work within a flow and contains all the functions and filters you
  # will apply to a pipe (Eaches), as well as group_by, union, and join.  For
  # aggregators and buffers, please see Aggregations.
  #
  # Function and filter DSL rules:
  # * Use positional arguments for required parameters
  # * Use params = {} for optional parameters
  # * Use *args sparingly, specifically when you need to accept a varying length list of fields
  # * If you require both a varying length list of fields and optional parameters, then see the Array#extract_options! extension
  # * If you choose to name a required parameter, add it to params = {} and throw an exception if the caller does not provide it
  # * If you have a require parameter that is provided by one of a set of params names, throw an exception if the caller does not provide at least one value (see :function and :filter in Assembly#each for an example)
  #
  # Function and filter DSL standard optional parameter names:
  # [input] c.p.Each argument selector
  # [into] c.o.Operation field declaration
  # [output] c.p.Each output selector
  class Assembly < Cascading::Node
    include Operations

    attr_reader :head_pipe, :tail_pipe

    def initialize(name, parent, outgoing_scopes = {})
      super(name, parent)

      @outgoing_scopes = outgoing_scopes
      if parent.kind_of?(Assembly)
        @head_pipe = Java::CascadingPipe::Pipe.new(name, parent.tail_pipe)
        # Copy to allow destructive update of name
        @outgoing_scopes[name] = parent.scope.copy
        scope.scope.name = name
      else # Parent is a Flow
        @head_pipe = Java::CascadingPipe::Pipe.new(name)
        @outgoing_scopes[name] ||= Scope.empty_scope(name)
      end
      @tail_pipe = head_pipe
      @incoming_scopes = [scope]
    end

    def describe(offset = '')
      incoming_scopes_desc = "#{@incoming_scopes.map{ |incoming_scope| incoming_scope.values_fields.to_a.inspect }.join(', ')}"
      incoming_scopes_desc = "(#{incoming_scopes_desc})" unless @incoming_scopes.size == 1
      description =  "#{offset}#{name}:assembly :: #{incoming_scopes_desc} -> #{scope.values_fields.to_a.inspect}"
      description += "\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}" unless children.empty?
      description
    end

    def parent_flow
      return parent if parent.kind_of?(Flow)
      parent.parent_flow
    end

    def scope
      @outgoing_scopes[name]
    end

    def debug_scope
      puts "Current scope for '#{name}':\n  #{scope}\n----------\n"
    end

    def make_pipe(type, parameters)
      @tail_pipe = type.new(*parameters)
      @outgoing_scopes[name] = Scope.outgoing_scope(tail_pipe, [scope])

      tail_pipe
    end
    private :make_pipe

    def populate_incoming_scopes(assembly_names, group_fields_args = {})
      # NOTE: this overrides the existing incoming_scopes, which changes the
      # way describe will function on this assembly
      pipes, @incoming_scopes, group_fields = [], [], []
      assembly_names.each do |assembly_name|
        assembly = parent_flow.find_child(assembly_name)
        raise "Could not find assembly '#{assembly_name}' from '#{name}'" unless assembly

        pipes << assembly.tail_pipe
        @incoming_scopes << assembly.scope
        group_fields << fields(group_fields_args[assembly_name]) if group_fields_args[assembly_name]
      end
      [pipes, group_fields]
    end
    private :populate_incoming_scopes

    def apply_aggregations(group, incoming_scopes, &block)
      aggregations = Aggregations.new(self, group, incoming_scopes)
      aggregations.instance_eval(&block) if block_given?

      # Sorting of any type means that we cannot use the AggregateBy optimization
      if aggregations.can_aggregate_by? && !group.is_sorted && !group.is_sort_reversed
        grouping_fields = group.key_selectors.values.first
        group.key_selectors.values.each do |key_fields|
          raise "Grouping fields mismatch: #{grouping_fields} expected; #{key_fields} found from #{group.key_selectors}" unless key_fields == grouping_fields
        end

        aggregate_by = sub_assembly(Java::CascadingPipeAssembly::AggregateBy.new(
          name,
          group.previous,
          grouping_fields,
          aggregations.aggregate_bys.to_java(Java::CascadingPipeAssembly::AggregateBy)
        ), group.previous, incoming_scopes)

        aggregate_by
      else
        aggregations.finalize if block_given?
        @tail_pipe = aggregations.tail_pipe
        @outgoing_scopes[name] = aggregations.scope

        group
      end
    end
    private :apply_aggregations

    def to_s
      "#{name} : head pipe : #{head_pipe} - tail pipe: #{tail_pipe}"
    end

    def prepare_join(assembly_names, params, &block)
      pipes, _ = populate_incoming_scopes(assembly_names)

      group_fields_args = params[:on]
      raise 'join requires :on parameter' unless group_fields_args

      if group_fields_args.kind_of?(String)
        group_fields_args = [group_fields_args]
      end

      group_fields = []
      if group_fields_args.kind_of?(Array)
        pipes.size.times do
          group_fields << fields(group_fields_args)
        end
      elsif group_fields_args.kind_of?(Hash)
        pipes, group_fields = populate_incoming_scopes(group_fields_args.keys.sort, group_fields_args)
      else
        raise "Unsupported data type for :on in join: '#{group_fields_args.class}'"
      end

      raise 'join requires non-empty :on parameter' if group_fields_args.empty?
      group_fields = group_fields.to_java(Java::CascadingTuple::Fields)
      incoming_fields = @incoming_scopes.map{ |s| s.values_fields }
      declared_fields = fields(params[:declared_fields] || dedup_fields(*incoming_fields))
      joiner = params[:joiner]
      is_hash_join = params[:hash] || false

      case joiner
      when :inner, 'inner', nil
        joiner = Java::CascadingPipeJoiner::InnerJoin.new
      when :left,  'left'
        joiner = Java::CascadingPipeJoiner::LeftJoin.new
      when :right, 'right'
        joiner = Java::CascadingPipeJoiner::RightJoin.new
      when :outer, 'outer'
        joiner = Java::CascadingPipeJoiner::OuterJoin.new
      when Array
        joiner = joiner.map do |t|
          case t
          when true,  1, :inner then true
          when false, 0, :outer then false
          else fail "invalid mixed joiner entry: #{t}"
          end
        end
        joiner = Java::CascadingPipeJoiner::MixedJoin.new(joiner.to_java(:boolean))
      end

      if is_hash_join
        raise ArgumentError, "hash joins don't support aggregations" if block_given?
        parameters = [
          pipes.to_java(Java::CascadingPipe::Pipe),
          group_fields,
          declared_fields,
          joiner
        ]
        group_assembly = Java::CascadingPipe::HashJoin.new(*parameters)
      else
        result_group_fields = dedup_fields(*group_fields)
        parameters = [
          pipes.to_java(Java::CascadingPipe::Pipe),
          group_fields,
          declared_fields,
          result_group_fields,
          joiner
        ]
        group_assembly = Java::CascadingPipe::CoGroup.new(*parameters)
      end
      apply_aggregations(group_assembly, @incoming_scopes, &block)
    end
    private :prepare_join

    # Builds a HashJoin pipe. This should be used carefully, as the right side
    # of the join is accumulated entirely in memory. Requires a list of assembly
    # names to join and :on to specify the join_fields.
    def hash_join(*args_with_params, &block)
      params, assembly_names = args_with_params.extract_options!, args_with_params
      params[:hash] = true
      prepare_join(assembly_names, params, &block)
    end

    # Builds a join (CoGroup) pipe. Requires a list of assembly names to join
    # and :on to specify the group_fields.
    def join(*args_with_params, &block)
      params, assembly_names = args_with_params.extract_options!, args_with_params
      params[:hash] = false
      prepare_join(assembly_names, params, &block)
    end
    alias co_group join

    def inner_join(*args_with_params, &block)
      params = args_with_params.extract_options!
      params[:joiner] = :inner
      args_with_params << params
      join(*args_with_params, &block)
    end

    def left_join(*args_with_params, &block)
      params = args_with_params.extract_options!
      params[:joiner] = :left
      args_with_params << params
      join(*args_with_params, &block)
    end

    def right_join(*args_with_params, &block)
      params = args_with_params.extract_options!
      params[:joiner] = :right
      args_with_params << params
      join(*args_with_params, &block)
    end

    def outer_join(*args_with_params, &block)
      params = args_with_params.extract_options!
      params[:joiner] = :outer
      args_with_params << params
      join(*args_with_params, &block)
    end

    # Builds a new branch.
    def branch(name, &block)
      raise "Could not build branch '#{name}'; block required" unless block_given?
      assembly = Assembly.new(name, self, @outgoing_scopes)
      add_child(assembly)
      assembly.instance_eval(&block)
      assembly
    end

    # Builds a new GroupBy pipe that groups on the fields given in
    # args_with_params. Any block passed to this method should contain only
    # Everies.
    def group_by(*args_with_params, &block)
      params, group_fields = args_with_params.extract_options!, fields(args_with_params)
      sort_fields = fields(params[:sort_by])
      reverse = params[:reverse]

      parameters = [tail_pipe, group_fields, sort_fields, reverse].compact
      apply_aggregations(Java::CascadingPipe::GroupBy.new(*parameters), [scope], &block)
    end

    # Unifies multiple incoming pipes sharing the same field structure using a
    # GroupBy.  Accepts :on like join and :sort_by and :reverse like group_by,
    # as well as a block which may be used for a sequence of Every
    # aggregations.
    #
    # By default, groups only on the first field (see line 189 of GroupBy.java)
    def union(*args_with_params, &block)
      params, assembly_names = args_with_params.extract_options!, args_with_params
      group_fields = fields(params[:on])
      sort_fields = fields(params[:sort_by])
      reverse = params[:reverse]

      pipes, _ = populate_incoming_scopes(assembly_names)

      # Must provide group_fields to ensure field name propagation
      group_fields = fields(@incoming_scopes.first.values_fields.get(0)) unless group_fields

      # FIXME: GroupBy is missing a constructor for union in wip-255
      sort_fields = group_fields if !sort_fields && !reverse.nil?

      parameters = [pipes.to_java(Java::CascadingPipe::Pipe), group_fields, sort_fields, reverse].compact
      apply_aggregations(Java::CascadingPipe::GroupBy.new(*parameters), @incoming_scopes, &block)
    end
    alias :union_pipes :union

    # Allows you to plugin c.p.SubAssemblies to a cascading.jruby Assembly
    # under certain assumptions.  Note the default is to extend the tail pipe
    # of this Assembly using a linear SubAssembly.  See SubAssembly class for
    # details.
    def sub_assembly(sub_assembly, pipes = [tail_pipe], incoming_scopes = [scope])
      sub_assembly = SubAssembly.new(self, sub_assembly)
      sub_assembly.finalize(pipes, incoming_scopes)

      @tail_pipe = sub_assembly.tail_pipe
      @outgoing_scopes[name] = sub_assembly.scope

      sub_assembly
    end

    # Builds a basic each pipe, and adds it to the current assembly.
    #
    # Default arguments are all_fields, a default inherited from c.o.Each.
    def each(*args_with_params)
      params, in_fields = args_with_params.extract_options!, fields(args_with_params)
      out_fields = fields(params[:output]) # Default Fields.RESULTS from c.o.Each
      operation = params[:filter] || params[:function]
      raise 'each requires either :filter or :function' unless operation
      raise 'c.p.Each does not support applying an output selector to a c.o.Filter' if params[:filter] && params[:output]

      parameters = [tail_pipe, in_fields, operation, out_fields].compact
      each = make_pipe(Java::CascadingPipe::Each, parameters)
      raise ':function specified but c.o.Filter provided' if params[:function] && each.is_filter
      raise ':filter specified but c.o.Function provided' if params[:filter] && each.is_function

      each
    end

    include IdentityOperations
    include RegexOperations
    include TextOperations

    def assert(assertion, params = {})
      assertion_level = params[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      parameters = [tail_pipe, assertion_level, assertion]
      make_pipe(Java::CascadingPipe::Each, parameters)
    end

    # Builds a pipe that assert the size of the tuple is the size specified in parameter.
    def assert_size_equals(size, params = {})
      assertion = Java::CascadingOperationAssertion::AssertSizeEquals.new(size)
      assert(assertion, params)
    end

    # Builds a pipe that assert the none of the fields in the tuple are null.
    def assert_not_null(params = {})
      assertion = Java::CascadingOperationAssertion::AssertNotNull.new
      assert(assertion, params)
    end

    # Builds a debugging pipe.
    #
    # Without arguments, it generate a simple debug pipe, that prints all tuple to the standard
    # output.
    #
    # The other named options are:
    # * <tt>:print_fields</tt> a boolean. If is set to true, then it prints every 10 tuples.
    #
    def debug(params = {})
      print_fields = params[:print_fields] || true
      debug = Java::CascadingOperation::Debug.new(print_fields)
      debug.print_tuple_every = params[:tuple_interval] || 1
      debug.print_fields_every = params[:fields_interval] || 10
      each(all_fields, :filter => debug)
    end

    # Builds a pipe that inserts values into the current tuple.
    #
    # The method takes a hash as parameter. This hash contains as keys the names of the fields to insert
    # and as values, the values they must contain. For example:
    #
    #       insert {"who" => "Grégoire", "when" => Time.now.strftime("%Y-%m-%d") }
    #
    # will insert two new fields: a field _who_ containing the string "Grégoire", and a field _when_ containing
    # the formatted current date.
    # The methods outputs all fields.
    # The named options are:
    def insert(args)
      args.keys.sort.each do |field_name|
        value = args[field_name]

        if value.kind_of?(ExprStub)
          value.validate_scope(scope)
          each all_fields, :function => expression_function(field_name, :expression => value.expression, :parameters => value.types), :output => all_fields
        else
          each all_fields, :function => insert_function([field_name], :values => [value]), :output => all_fields
        end
      end
    end

    # Builds a pipe that filters the tuples based on an expression or a pattern (but not both !).
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:pattern</tt> a string. Specifies a regular expression pattern used to filter the tuples. If this
    # option is provided, then the filter is regular expression-based. This is incompatible with the _expression_ option.
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to filter the tuples. This option has the
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino
    # expression-based. This is incompatible with the _pattern_ option.
    # * <tt>:validate</tt> a boolean.  Passed into Cascading#expr to enable or disable
    # expression validation.  Defaults to true.
    # * <tt>:validate_with</tt> a hash.  Actual arguments used by Cascading#expr for
    # expression validation.  Defaults to {}.
    def filter(*args)
      options = args.extract_options!
      from = options.delete(:from) || all_fields
      expression = options.delete(:expression) || args.shift
      regex = options.delete(:pattern)
      validate = options.has_key?(:validate) ? options.delete(:validate) : true
      validate_with = options.has_key?(:validate_with) ? options.delete(:validate_with) : {}

      if expression
        stub = expr(expression, { :validate => validate, :validate_with => validate_with })
        types, expression = stub.types, stub.expression

        stub.validate_scope(scope)
        each from, :filter => expression_filter(
          :parameters => types,
          :expression => expression
        )
      elsif regex
        each from, :filter => regex_filter(regex, options)
      end
    end

    def filter_null(*args)
      options = args.extract_options!
      each(args, :filter => Java::CascadingOperationFilter::FilterNull.new)
    end
    alias reject_null filter_null

    def filter_not_null(*args)
      options = args.extract_options!
      each(args, :filter => Java::CascadingOperationFilter::FilterNotNull.new)
    end
    alias where_null filter_not_null

    # Builds a pipe that rejects the tuples based on an expression.
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to filter the tuples. This option has the
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino
    # expression-based.
    # * <tt>:validate</tt> a boolean.  Passed into Cascading#expr to enable or disable
    # expression validation.  Defaults to true.
    # * <tt>:validate_with</tt> a hash.  Actual arguments used by Cascading#expr for
    # expression validation.  Defaults to {}.
    def reject(*args)
      options = args.extract_options
      raise "Regex not allowed" if options && options[:pattern]

      filter(*args)
    end

    # Builds a pipe that includes just the tuples matching an expression.
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to select the tuples. This option has the
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino
    # expression-based.
    # * <tt>:validate</tt> a boolean.  Passed into Cascading#expr to enable or disable
    # expression validation.  Defaults to true.
    # * <tt>:validate_with</tt> a hash.  Actual arguments used by Cascading#expr for
    # expression validation.  Defaults to {}.
    def where(*args)
      options = args.extract_options
      raise "Regex not allowed" if options && options[:pattern]

      if options[:expression]
        _, imports, expr = options[:expression].match(/^((?:\s*import.*;\s*)*)(.*)$/).to_a
        options[:expression] = "#{imports}!(#{expr})"
      elsif args[0]
        _, imports, expr = args[0].match(/^((?:\s*import.*;\s*)*)(.*)$/).to_a
        args[0] = "#{imports}!(#{expr})"
      end

      filter(*args)
    end

    # Builds a pipe that evaluates the specified Janino expression and insert it in a new field in the tuple.
    #
    # The named options are:
    # * <tt>:from</tt> a string or array of strings. Specifies the input fields.
    # * <tt>:express</tt> a string. The janino expression.
    # * <tt>:into</tt> a string. Specified the name of the field to insert with the result of the evaluation.
    # * <tt>:parameters</tt> a hash. Specifies the type mapping for the parameters. See Cascading::Operations.expression_function.
    def eval_expression(*args)
      options = args.extract_options!

      into = options.delete(:into)
      from = options.delete(:from) || all_fields
      output = options.delete(:output) || all_fields
      options[:expression] ||= args.shift
      options[:parameters] ||= args.shift

      each from, :function => expression_function(into, options), :output=>output
    end

    # Builds a pipe that returns distinct tuples based on the provided fields.
    #
    # The method accepts optional unamed argument specifying the fields to base the distinct on
    # (all fields, by default).
    def distinct(*args)
      raise "Distinct is badly broken"
      fields = args[0] || all_fields
      group_by *fields
      pass
    end

    # Ungroups, or unpivots, a tuple (see Cascading's UnGroup at http://docs.cascading.org/cascading/2.0/javadoc/cascading/operation/function/UnGroup.html).
    #
    # You must provide :key and you must provide only one of :value_selectors
    # and :num_values.
    #
    # The named options are:
    # * <tt>:key</tt> required array of field names to replicate on every
    #   output row in an ungrouped group.
    # * <tt>:value_selectors</tt> an array of field names to ungroup.  Each
    #   field will be ungrouped into an output tuple along with the key fields
    #   in the order provided.
    # * <tt>:num_values</tt> an integer specifying the number of fields to
    #   ungroup into each output tuple (excluding the key fields).  All input
    #   fields will be ungrouped.
    # * <tt>:input</tt> an array of field names that specifies the fields to
    #   input to UnGroup.  Defaults to all_fields.
    # * <tt>:into</tt> an array of field names.  Default set by UnGroup.
    # * <tt>:output</tt> an array of field names that specifies the fields to
    #   produce as output of UnGroup.  Defaults to all_fields.
    def ungroup(*args)
      options = args.extract_options!
      input = options[:input] || all_fields
      into = fields(options[:into])
      output = options[:output] || all_fields
      key = fields(options[:key])

      raise 'You must provide exactly one of :value_selectors or :num_values to ungroup' unless options.has_key?(:value_selectors) ^ options.has_key?(:num_values)
      value_selectors = options[:value_selectors].map{ |vs| fields(vs) }.to_java(Java::CascadingTuple::Fields) if options.has_key?(:value_selectors)
      num_values = options[:num_values] if options.has_key?(:num_values)

      parameters = [into, key, value_selectors, num_values].compact
      each input, :function => Java::CascadingOperationFunction::UnGroup.new(*parameters), :output => output
    end

    # Inserts one of two values into the dataflow based upon the result of the
    # supplied filter on the input fields.  This is primarily useful for
    # creating indicators from filters.
    #
    # Parameters:
    # * <tt>input</tt> name of field to apply the filter.
    # * <tt>filter</tt> Cascading Filter to apply.
    # * <tt>keep_value</tt> Java value to produce when the filter would keep
    #   the given input.
    # * <tt>remove_value</tt> Java value to produce when the filter would
    #   remove the given input.
    #
    # The named options are:
    # * <tt>:into</tt> an output field name, defaulting to 'filter_value'.
    # * <tt>:output</tt> an array of field names that specifies the fields to
    #   retain in the output tuple.  Defaults to all_fields.
    def set_value(input, filter, keep_value, remove_value, params = {})
      into = fields(params[:into] || 'filter_value')
      output = params[:output] || all_fields
      each input, :function => Java::CascadingOperationFunction::SetValue.new(into, filter, keep_value, remove_value), :output => output
    end

    # Efficient way of inserting a null indicator for any field, even one that
    # cannot be coerced to a string.  This is accomplished using Cascading's
    # FilterNull and SetValue operators rather than Janino.  1 is produced if
    # the field is null and 0 otherwise.
    #
    # Parameters:
    # * <tt>input</tt> name of field to check for null.
    #
    # The named options are:
    # * <tt>:into</tt> an output field name, defaulting to 'is_null'.
    # * <tt>:output</tt> an array of field names that specifies the fields to
    #   retain in the output tuple.  Defaults to all_fields.
    def null_indicator(input, params = {})
      into = fields(params[:into] || 'is_null')
      output = params[:output] || all_fields
      set_value input, Java::CascadingOperationFilter::FilterNull.new, 1.to_java, 0.to_java, :into => into, :output => output
    end

    # Given a field and a regex, returns an indicator that is 1 if the string
    # contains at least 1 match and 0 otherwise.
    #
    # Parameters:
    # * <tt>input</tt> field name or names that specifies the fields over which
    #   to perform the match.
    # * <tt>pattern</tt> regex to apply to the input.
    #
    # The named options are:
    # * <tt>:into</tt> an output field name, defaulting to 'regex_contains'.
    # * <tt>:output</tt> an array of field names that specifies the fields to
    #   retain in the output tuple.  Defaults to all_fields.
    def regex_contains(input, pattern, params = {})
      input = fields(input)
      pattern = pattern.to_s # Supports JRuby regexes
      into = fields(params[:into] || 'regex_contains')
      output = params[:output] || all_fields
      set_value input, Java::CascadingOperationRegex::RegexFilter.new(pattern), 1.to_java, 0.to_java, :into => into, :output => output
    end
  end
end
