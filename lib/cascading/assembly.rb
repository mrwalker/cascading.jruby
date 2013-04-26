require 'cascading/base'
require 'cascading/operations'
require 'cascading/identity_operations'
require 'cascading/filter_operations'
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
  # * Use options = {} for optional parameters
  # * Use *args sparingly, specifically when you need to accept a varying length list of fields
  # * If you require both a varying length list of fields and optional parameters, then see the Array#extract_options! extension
  # * If you choose to name a required parameter, add it to options = {} and throw an exception if the caller does not provide it
  # * If you have a require parameter that is provided by one of a set of options names, throw an exception if the caller does not provide at least one value (see :function and :filter in Assembly#each for an example)
  #
  # Function and filter DSL standard optional parameter names:
  # [input] c.p.Each argument selector
  # [into] c.o.Operation field declaration
  # [output] c.p.Each output selector
  #
  # A note on aliases: when a DSL method uniquely wraps a single Cascading
  # operation, we attempt to provide an alias that matches the Cascading
  # operation.  However, Cascading operations are often nouns rather than verbs,
  # and the latter are preferable for a dataflow DSL.
  class Assembly < Cascading::Node
    attr_reader :head_pipe, :tail_pipe

    # Do not use this constructor directly; instead, use Flow#assembly or
    # Assembly#branch to build assemblies.
    #
    # Builds an Assembly given a name, parent, and optional outgoing_scopes
    # (necessary only for branching).
    #
    # An assembly's name is quite important as it will determine:
    # * The sources from which it will read, if any
    # * The name to be used in joins or unions downstream
    # * The name to be used to sink the output of the assembly downstream
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

    # Produces a textual description of this Assembly.  The description details
    # the structure of the Assembly, its input and output fields and any
    # children (branches).  The offset parameter allows for this describe to be
    # nested within a calling context, which lets us indent the structural
    # hierarchy of a job.
    def describe(offset = '')
      incoming_scopes_desc = "#{@incoming_scopes.map{ |incoming_scope| incoming_scope.values_fields.to_a.inspect }.join(', ')}"
      incoming_scopes_desc = "(#{incoming_scopes_desc})" unless @incoming_scopes.size == 1
      description =  "#{offset}#{name}:assembly :: #{incoming_scopes_desc} -> #{scope.values_fields.to_a.inspect}"
      description += "\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}" unless children.empty?
      description
    end

    # Rather than the immediate parent, this method returns the parent flow of
    # this Assembly.  If this is a branch, we must traverse the parents of
    # parent assemblies.
    def parent_flow
      return parent if parent.kind_of?(Flow)
      parent.parent_flow
    end

    # Accesses the outgoing scope of this Assembly at the point at which it is
    # called.  This is useful for grabbing the values_fields at any point in
    # the construction of the Assembly.  See Scope for details.
    def scope
      @outgoing_scopes[name]
    end

    # Prints information about the scope of this Assembly at the point at which
    # it is called.  This allows you to trace the propagation of field names
    # through your job and is handy for debugging.  See Scope for details.
    def debug_scope
      puts "Current scope for '#{name}':\n  #{scope}\n----------\n"
    end

    # Prints detail about this Assembly including its name, head pipe, and tail
    # pipe.
    def to_s
      "#{name} : head pipe : #{head_pipe} - tail pipe: #{tail_pipe}"
    end

    # Builds a HashJoin pipe. This should be used carefully, as the right side
    # of the join is accumulated entirely in memory. Requires a list of
    # assembly names to join and :on to specify the join_fields.  Note that a
    # hash_join "takes over" the Assembly in which it is built, so it is
    # typically the first statement within the block of the assembly or branch.
    # Additionally, a hash join does not accept a block for aggregations like
    # other joins; this restriction is enforced here, but comes directly from
    # Cascading.
    #
    # The named options are:
    # [on] The keys of the join, an array of strings if they are the same in
    #      all inputs, or a hash mapping assembly names to key names if they
    #      differ across inputs.
    # [declared_fields] By default, a deduplicated array of incoming field
    #                   names (see Cascading::dedup_fields).  Specifies the
    #                   names of the fields that will be available to
    #                   aggregations or post-join if no aggregations are
    #                   specified.
    # [joiner] A specification of the c.p.j.Joiner to use.  Values like :inner
    #          and 'inner', :right and 'right' are accepted, as well as an
    #          array specifying mixed joins.  Typically, this is not provided,
    #          but one of the higher level join methods on Assembly is used
    #          directly (like Assembly#inner_join or Assembly#right_join).
    #
    # Example:
    #     assembly 'join_left_right' do
    #       hash_join 'left', 'right', :on => ['key1', 'key2'], :joiner => :inner
    #     end
    def hash_join(*args_with_options)
      raise ArgumentError, "HashJoin doesn't support aggregations so the block provided to hash_join will be ignored" if block_given?

      options, assembly_names = args_with_options.extract_options!, args_with_options
      options[:hash] = true
      prepare_join(assembly_names, options)
    end

    # Builds a join (CoGroup) pipe. Requires a list of assembly names to join
    # and :on to specify the group_fields.  Note that a join "takes over" the
    # Assembly in which it is built, so it is typically the first statement
    # within the block of the assembly or branch.  The block passed to this
    # method will be evaluated in the context of Aggregations, not Assembly.
    #
    # The named options are:
    # [on] The keys of the join, an array of strings if they are the same in
    #      all inputs, or a hash mapping assembly names to key names if they
    #      differ across inputs.
    # [declared_fields] By default, a deduplicated array of incoming field
    #                   names (see Cascading::dedup_fields).  Specifies the
    #                   names of the fields that will be available to
    #                   aggregations or post-join if no aggregations are
    #                   specified.
    # [joiner] A specification of the c.p.j.Joiner to use.  Values like :inner
    #          and 'inner', :right and 'right' are accepted, as well as an
    #          array specifying mixed joins.  Typically, this is not provided,
    #          but one of the higher level join methods on Assembly is used
    #          directly (like Assembly#inner_join or Assembly#right_join).
    #
    # Example:
    #     assembly 'join_left_right' do
    #       join 'left', 'right', :on => ['key1', 'key2'], :joiner => :inner do
    #         sum 'val1', 'val2', :type => :long
    #       end
    #     end
    def join(*args_with_options, &block)
      options, assembly_names = args_with_options.extract_options!, args_with_options
      options[:hash] = false
      prepare_join(assembly_names, options, &block)
    end
    alias co_group join

    # Builds an inner join (CoGroup) pipe. Requires a list of assembly names to
    # join and :on to specify the group_fields.
    #
    # The named options are:
    # [on] The keys of the join, an array of strings if they are the same in
    #      all inputs, or a hash mapping assembly names to key names if they
    #      differ across inputs.
    # [declared_fields] By default, a deduplicated array of incoming field
    #                   names (see Cascading::dedup_fields).  Specifies the
    #                   names of the fields that will be available to
    #                   aggregations or post-join if no aggregations are
    #                   specified.
    #
    # Example:
    #     assembly 'join_left_right' do
    #       inner_join 'left', 'right', :on => ['key1', 'key2']
    #         sum 'val1', 'val2', :type => :long
    #       end
    #     end
    def inner_join(*args_with_options, &block)
      options = args_with_options.extract_options!
      options[:joiner] = :inner
      args_with_options << options
      join(*args_with_options, &block)
    end

    # Builds a left join (CoGroup) pipe. Requires a list of assembly names to
    # join and :on to specify the group_fields.
    #
    # The named options are:
    # [on] The keys of the join, an array of strings if they are the same in
    #      all inputs, or a hash mapping assembly names to key names if they
    #      differ across inputs.
    # [declared_fields] By default, a deduplicated array of incoming field
    #                   names (see Cascading::dedup_fields).  Specifies the
    #                   names of the fields that will be available to
    #                   aggregations or post-join if no aggregations are
    #                   specified.
    #
    # Example:
    #     assembly 'join_left_right' do
    #       left_join 'left', 'right', :on => ['key1', 'key2'] do
    #         sum 'val1', 'val2', :type => :long
    #       end
    #     end
    def left_join(*args_with_options, &block)
      options = args_with_options.extract_options!
      options[:joiner] = :left
      args_with_options << options
      join(*args_with_options, &block)
    end

    # Builds a right join (CoGroup) pipe. Requires a list of assembly names to
    # join and :on to specify the group_fields.
    #
    # The named options are:
    # [on] The keys of the join, an array of strings if they are the same in
    #      all inputs, or a hash mapping assembly names to key names if they
    #      differ across inputs.
    # [declared_fields] By default, a deduplicated array of incoming field
    #                   names (see Cascading::dedup_fields).  Specifies the
    #                   names of the fields that will be available to
    #                   aggregations or post-join if no aggregations are
    #                   specified.
    #
    # Example:
    #     assembly 'join_left_right' do
    #       right_join 'left', 'right', :on => ['key1', 'key2'] do
    #         sum 'val1', 'val2', :type => :long
    #       end
    #     end
    def right_join(*args_with_options, &block)
      options = args_with_options.extract_options!
      options[:joiner] = :right
      args_with_options << options
      join(*args_with_options, &block)
    end

    # Builds an outer join (CoGroup) pipe. Requires a list of assembly names to
    # join and :on to specify the group_fields.
    #
    # The named options are:
    # [on] The keys of the join, an array of strings if they are the same in
    #      all inputs, or a hash mapping assembly names to key names if they
    #      differ across inputs.
    # [declared_fields] By default, a deduplicated array of incoming field
    #                   names (see Cascading::dedup_fields).  Specifies the
    #                   names of the fields that will be available to
    #                   aggregations or post-join if no aggregations are
    #                   specified.
    #
    # Example:
    #     assembly 'join_left_right' do
    #       outer_join 'left', 'right', :on => ['key1', 'key2'] do
    #         sum 'val1', 'val2', :type => :long
    #       end
    #     end
    def outer_join(*args_with_options, &block)
      options = args_with_options.extract_options!
      options[:joiner] = :outer
      args_with_options << options
      join(*args_with_options, &block)
    end

    # Builds a child Assembly that branches this Assembly given a name and
    # block.
    #
    # An assembly's name is quite important as it will determine:
    # * The sources from which it will read, if any
    # * The name to be used in joins or unions downstream
    # * The name to be used to sink the output of the assembly downstream
    #
    # Many branches may be built within an assembly.  The result of a branch is
    # the same as the Flow#assembly constructor, an Assembly object.
    #
    # Example:
    #     assembly 'some_work' do
    #       ...
    #
    #       branch 'more_work' do
    #         ...
    #       end
    #
    #       branch 'yet_more_work' do
    #         ...
    #       end
    #     end
    def branch(name, &block)
      raise "Could not build branch '#{name}'; block required" unless block_given?
      assembly = Assembly.new(name, self, @outgoing_scopes)
      add_child(assembly)
      assembly.instance_eval(&block)
      assembly
    end

    # Builds a new GroupBy pipe that groups on the fields given in
    # args_with_options. The block passed to this method will be evaluated in
    # the context of Aggregations, not Assembly.
    #
    # The named options are:
    # [sort_by] Optional keys for within-group sort.
    # [reverse] Boolean that can reverse the order of within-group sorting
    #           (only makes sense given :sort_by keys).
    #
    # Example:
    #     assembly 'total' do
    #       ...
    #       insert 'const' => 1
    #       group_by 'const' do
    #         count
    #         sum 'val1', 'val2', :type => :long
    #       end
    #       discard 'const'
    #     end
    def group_by(*args_with_options, &block)
      options, group_fields = args_with_options.extract_options!, fields(args_with_options)
      sort_fields = fields(options[:sort_by])
      reverse = options[:reverse]

      parameters = [tail_pipe, group_fields, sort_fields, reverse].compact
      apply_aggregations(Java::CascadingPipe::GroupBy.new(*parameters), [scope], &block)
    end

    # Unifies multiple incoming pipes sharing the same field structure using a
    # GroupBy.  Accepts :on like join and :sort_by and :reverse like group_by,
    # as well as a block which may be used for a sequence of Every
    # aggregations.  The block passed to this method will be evaluated in the
    # context of Aggregations, not Assembly.
    #
    # By default, groups only on the first field (see line 189 of GroupBy.java)
    #
    # The named options are:
    # [on] The keys of the union, which defaults to the first field in the
    #      first input assembly.
    # [sort_by] Optional keys for sorting.
    # [reverse] Boolean that can reverse the order of sorting
    #           (only makes sense given :sort_by keys).
    #
    # Example:
    #     assembly 'union_left_right' do
    #       union 'left', 'right' do
    #         sum 'val1', 'val2', :type => :long
    #       end
    #     end
    def union(*args_with_options, &block)
      options, assembly_names = args_with_options.extract_options!, args_with_options
      group_fields = fields(options[:on])
      sort_fields = fields(options[:sort_by])
      reverse = options[:reverse]

      pipes, _ = populate_incoming_scopes(assembly_names)

      # Must provide group_fields to ensure field name propagation
      group_fields = fields(@incoming_scopes.first.values_fields.get(0)) unless group_fields

      # FIXME: GroupBy is missing a constructor for union in wip-255
      sort_fields = group_fields if !sort_fields && !reverse.nil?

      parameters = [pipes.to_java(Java::CascadingPipe::Pipe), group_fields, sort_fields, reverse].compact
      apply_aggregations(Java::CascadingPipe::GroupBy.new(*parameters), @incoming_scopes, &block)
    end
    alias :union_pipes :union

    # Allows you to plugin c.p.SubAssemblies to an Assembly under certain
    # assumptions.  Note the default is to extend the tail pipe of this
    # Assembly using a linear SubAssembly.  See SubAssembly class for details.
    #
    # Example:
    #     assembly 'id_rows' do
    #       ...
    #       sub_assembly Java::CascadingPipeAssembly::Discard.new(tail_pipe, fields('id'))
    #     end
    def sub_assembly(sub_assembly, pipes = [tail_pipe], incoming_scopes = [scope])
      sub_assembly = SubAssembly.new(self, sub_assembly)
      sub_assembly.finalize(pipes, incoming_scopes)

      @tail_pipe = sub_assembly.tail_pipe
      @outgoing_scopes[name] = sub_assembly.scope

      sub_assembly
    end

    # Builds a basic each pipe and adds it to the current Assembly.  Default
    # arguments are all_fields, a default inherited from c.o.Each.  Exactly one
    # of :function and :filter must be specified and filters do not support an
    # :output selector.
    #
    # The named options are:
    # [filter] A Cascading Filter, mutually exclusive with :function.
    # [function] A Cascading Function, mutually exclusive with :filter.
    # [output] c.p.Each output selector, only valid with :function.
    #
    # Example:
    #    each fields(input_fields), :function => Java::CascadingOperation::Identity.new
    #    each 'field1', 'field2', :function => Java::CascadingOperation::Identity.new
    def each(*args_with_options)
      options, in_fields = args_with_options.extract_options!, fields(args_with_options)
      out_fields = fields(options[:output]) # Default Fields.RESULTS from c.o.Each
      operation = options[:filter] || options[:function]
      raise 'each requires either :filter or :function' unless operation
      raise 'c.p.Each does not support applying an output selector to a c.o.Filter' if options[:filter] && options[:output]

      parameters = [tail_pipe, in_fields, operation, out_fields].compact
      each = make_pipe(Java::CascadingPipe::Each, parameters)
      raise ':function specified but c.o.Filter provided' if options[:function] && each.is_filter
      raise ':filter specified but c.o.Function provided' if options[:filter] && each.is_function

      each
    end

    include Operations
    include IdentityOperations
    include FilterOperations
    include RegexOperations
    include TextOperations

    # Builds an each assertion pipe given a c.o.a.Assertion and adds it to the
    # current Assembly.
    #
    # The named options are:
    # [level] The assertion level; defaults to strict.
    def assert(assertion, options = {})
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      parameters = [tail_pipe, assertion_level, assertion]
      make_pipe(Java::CascadingPipe::Each, parameters)
    end

    # Builds a pipe that asserts the size of the tuple is the specified size.
    def assert_size_equals(size, options = {})
      assertion = Java::CascadingOperationAssertion::AssertSizeEquals.new(size)
      assert(assertion, options)
    end

    # Builes a pipe that asserts none of the fiels in the tuple are null.
    def assert_not_null(options = {})
      assertion = Java::CascadingOperationAssertion::AssertNotNull.new
      assert(assertion, options)
    end

    private

    def make_pipe(type, parameters)
      @tail_pipe = type.new(*parameters)
      @outgoing_scopes[name] = Scope.outgoing_scope(tail_pipe, [scope])

      tail_pipe
    end

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

    def prepare_join(assembly_names, options, &block)
      pipes, _ = populate_incoming_scopes(assembly_names)

      group_fields_args = options[:on]
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
      declared_fields = fields(options[:declared_fields] || dedup_fields(*incoming_fields))
      joiner = options[:joiner]
      is_hash_join = options[:hash] || false

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
  end
end
