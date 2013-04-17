module Cascading
  module Operations
    # Debugs the current assembly at runtime, printing every tuple and fields
    # every 10 tuples by default.
    #
    # The named params are:
    # [prefix] String to prefix prints with.
    # [print_fields] Boolean controlling field printing, defaults to false.
    # [tuple_interval] Integer specifying interval between printed tuples
    # [fields_interval] Integer specifying interval between printing fields
    def debug(params = {})
      input_fields = params[:input] || all_fields
      prefix = params[:prefix]
      print_fields = params[:print_fields]

      parameters = [prefix, print_fields].compact
      debug = Java::CascadingOperation::Debug.new(*parameters)

      debug.print_tuple_every = params[:tuple_interval] || 1
      debug.print_fields_every = params[:fields_interval] || 10

      each(input_fields, :filter => debug)
    end

    # Inserts new fields into the current assembly.  Values may be constants or
    # expressions (see Cascading::expr).  Fields will be inserted in
    # lexicographic order (not necessarily the order provided).
    #
    # Example:
    #     insert 'field1' => 'constant_string', 'field2' => 0, 'field3' => expr('fieldA:long + fieldB:long')
    def insert(insert_map)
      insert_map.keys.sort.each do |field_name|
        value = insert_map[field_name]

        if value.kind_of?(ExprStub)
          value.validate_scope(scope)
          names, types = value.names_and_types
          each(
            all_fields,
            :function => Java::CascadingOperationExpression::ExpressionFunction.new(fields(field_name), value.expression, names, types),
            :output => all_fields
          )
        else # value is a constant
          each(
            all_fields,
            :function => Java::CascadingOperation::Insert.new(fields(field_name), to_java_comparable_array([value])),
            :output => all_fields
          )
        end
      end
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

    private

    def to_java_comparable_array(arr)
      (arr.map do |v|
        coerce_to_java(v)
      end).to_java(java.lang.Comparable)
    end

    def coerce_to_java(v)
      case v
        when Fixnum
          java.lang.Long.new(v)
        when Float
          java.lang.Double.new(v)
        when NilClass
          nil
        else
          java.lang.String.new(v.to_s)
      end
    end
  end
end
