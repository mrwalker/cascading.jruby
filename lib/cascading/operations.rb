module Cascading
  module Operations
    # Debugs the current assembly at runtime, printing every tuple and fields
    # every 10 tuples by default.
    #
    # The named options are:
    # [prefix] String to prefix prints with.
    # [print_fields] Boolean controlling field printing, defaults to false.
    # [tuple_interval] Integer specifying interval between printed tuples
    # [fields_interval] Integer specifying interval between printing fields
    #
    # Example:
    #     debug :prefix => 'DEBUG', :print_fields => true, :fields_interval => 1000
    def debug(options = {})
      input_fields = options[:input] || all_fields
      prefix = options[:prefix]
      print_fields = options[:print_fields]

      debug = Java::CascadingOperation::Debug.new(*[prefix, print_fields].compact)

      debug.print_tuple_every = options[:tuple_interval] || 1
      debug.print_fields_every = options[:fields_interval] || 10

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

    # Ungroups, or unpivots, a tuple (see Cascading's {UnGroup}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/function/UnGroup.html]).
    #
    # You must provide exactly one of :value_selectors and :num_values.
    #
    # The named options are:
    # [value_selectors] Array of field names to ungroup. Each field will be
    #                   ungrouped into an output tuple along with the key fields
    #                   in the order provided.
    # [num_values] Integer specifying the number of fields to ungroup into each
    #              output tuple (excluding the key fields).  All input fields
    #              will be ungrouped.
    #
    # Example:
    #     ungroup 'key', ['new_key', 'val], :value_selectors => ['val1', 'val2', 'val3'], :output => ['new_key', 'val']
    def ungroup(key, into_fields, options = {})
      input_fields = options[:input] || all_fields
      output = options[:output] || all_fields

      raise 'You must provide exactly one of :value_selectors or :num_values to ungroup' unless options.has_key?(:value_selectors) ^ options.has_key?(:num_values)
      value_selectors = options[:value_selectors].map{ |vs| fields(vs) }.to_java(Java::CascadingTuple::Fields) if options.has_key?(:value_selectors)
      num_values = options[:num_values] if options.has_key?(:num_values)

      parameters = [fields(into_fields), fields(key), value_selectors, num_values].compact
      each input_fields, :function => Java::CascadingOperationFunction::UnGroup.new(*parameters), :output => output
    end

    # Inserts one of two values into the dataflow based upon the result of the
    # supplied filter on the input_fields.  This is primarily useful for
    # creating indicators from filters.  keep_value specifies the Java value to
    # produce when the filter would keep the given input and remove_value
    # specifies the Java value to produce when the filter would remove the given
    # input.
    #
    # Example:
    #     set_value 'field1', Java::CascadingOperationFilter::FilterNull.new, 1.to_java, 0.to_java, 'is_field1_null'
    def set_value(input_fields, filter, keep_value, remove_value, into_field, options = {})
      output = options[:output] || all_fields
      each input_fields, :function => Java::CascadingOperationFunction::SetValue.new(fields(into_field), filter, keep_value, remove_value), :output => output
    end

    # Efficient way of inserting a null indicator for any field, even one that
    # cannot be coerced to a string.  This is accomplished using Cascading's
    # FilterNull and SetValue operators rather than Janino.  1 is produced if
    # the field is null and 0 otherwise.
    #
    # Example:
    #     null_indicator 'field1', 'is_field1_null'
    def null_indicator(input_field, into_field, options = {})
      set_value input_field, Java::CascadingOperationFilter::FilterNull.new, 1.to_java, 0.to_java, into_field, :output => options[:output]
    end

    # Given an input_field and a regex, returns an indicator that is 1 if the string
    # contains at least 1 match and 0 otherwise.
    #
    # Example:
    #     regex_contains 'field1', /\w+\s+\w+/, 'does_field1_contain_pair'
    def regex_contains(input_field, regex, into_field, options = {})
      set_value input_field, Java::CascadingOperationRegex::RegexFilter.new(pattern.to_s), 1.to_java, 0.to_java, into_field, :output => options[:output]
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
