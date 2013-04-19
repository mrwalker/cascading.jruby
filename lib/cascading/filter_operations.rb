module Cascading
  # Module of filtering operations.  Unlike some of the other functional
  # operations modules, this one does not just wrap operations defined by
  # Cascading in cascading.operation.filter.  Instead, it provides some useful
  # high-level DSL pipes which map many Cascading operations into a smaller
  # number of DSL statements.
  #
  # Still, some are direct wrappers:
  # filter\_null:: {FilterNull}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/filter/FilterNull.html]
  # filter\_not\_null:: {FilterNotNull}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/filter/FilterNotNull.html]
  module FilterOperations
    # Filter the current assembly based on an expression or regex, but not both.
    #
    # The named options are:
    # [expression] A Janino expression used to filter.  Has access to all :input
    #              fields.
    # [validate] Boolean passed to Cascading#expr to enable or disable
    #            expression validation.  Defaults to true.
    # [validate_with] Hash mapping field names to actual arguments used by
    #                 Cascading#expr for expression validation.  Defaults to {}.
    # [regex] A regular expression used to filter.
    # [remove_match] Boolean indicating if regex matches should be removed or
    #                kept.  Defaults to false, which is a bit counterintuitive.
    # [match_each_element] Boolean indicating if regex should match entire
    #                      incoming tuple (joined with tabs) or each field
    #                      individually.  Defaults to false.
    #
    # Example:
    #     filter :input => 'field1', :regex => /\t/, :remove_match => true
    #     filter :expression => 'field1:long > 0 && "".equals(field2:string)', :remove_match => true
    def filter(options = {})
      input_fields = options[:input] || all_fields
      expression = options[:expression]
      regex = options[:regex]
      validate = options.has_key?(:validate) ? options[:validate] : true
      validate_with = options[:validate_with] || {}

      if expression
        stub = expr(expression, { :validate => validate, :validate_with => validate_with })
        stub.validate_scope(scope)

        names, types = stub.names_and_types
        each input_fields, :filter => Java::CascadingOperationExpression::ExpressionFilter.new(
            stub.expression,
            names,
            types
          )
      elsif regex
        parameters = [regex.to_s, options[:remove_match], options[:match_each_element]].compact
        each input_fields, :filter => Java::CascadingOperationRegex::RegexFilter.new(*parameters)
      else
        raise 'filter requires one of :expression or :regex'
      end
    end

    # Rejects tuples from the current assembly based on a Janino expression.
    # This is just a wrapper for FilterOperations.filter.
    #
    # Example:
    #     reject 'field1:long > 0 && "".equals(field2:string)'
    def reject(expression, options = {})
      options[:expression] = expression
      filter(options)
    end

    # Keeps tuples from the current assembly based on a Janino expression.  This
    # is a wrapper for FilterOperations.filter.
    #
    # Note that this is accomplished by inverting the given expression, and best
    # attempt is made to support import statements prior to the expression.  If
    # this support should break, simply negate your expression and use
    # FilterOperations.reject.
    #
    # Example:
    #     where 'field1:long > 0 && "".equals(field2:string)'
    def where(expression, options = {})
      _, imports, expr = expression.match(/^((?:\s*import.*;\s*)*)(.*)$/).to_a
      options[:expression] = "#{imports}!(#{expr})"
      filter(options)
    end

    # Rejects tuples from the current assembly if any input field is null.
    #
    # Example:
    #     filter_null 'field1', 'field2'
    def filter_null(*input_fields)
      each(input_fields, :filter => Java::CascadingOperationFilter::FilterNull.new)
    end
    alias reject_null filter_null

    # Rejects tuples from the current assembly if any input field is not null.
    #
    # Example:
    #     filter_not_null 'field1', 'field2'
    def filter_not_null(*input_fields)
      each(input_fields, :filter => Java::CascadingOperationFilter::FilterNotNull.new)
    end
    alias where_null filter_not_null
  end
end
