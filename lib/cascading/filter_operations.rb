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
    # The named params are:
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
    def filter(params = {})
      input_fields = params[:input] || all_fields
      expression = params[:expression]
      regex = params[:regex]
      validate = params.has_key?(:validate) ? params[:validate] : true
      validate_with = params[:validate_with] || {}

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
        parameters = [regex.to_s, params[:remove_match], params[:match_each_element]].compact
        each input_fields, :filter => Java::CascadingOperationRegex::RegexFilter.new(*parameters)
      else
        raise 'filter requires one of :expression or :regex'
      end
    end

    # Rejects tuples from the current assembly based on a Janino expression.
    # This is just a wrapper for FilterOperations.filter.
    def reject(expression, params = {})
      params[:expression] = expression
      filter(params)
    end

    # Keeps tuples from the current assembly based on a Janino expression.  This
    # is a wrapper for FilterOperations.filter.
    #
    # Note that this is accomplished by inverting the given expression, and best
    # attempt is made to support import statements prior to the expression.  If
    # this support should break, simply negate your expression and use
    # FilterOperations.reject.
    def where(expression, params = {})
      _, imports, expr = expression.match(/^((?:\s*import.*;\s*)*)(.*)$/).to_a
      params[:expression] = "#{imports}!(#{expr})"
      filter(params)
    end

    # Rejects tuples from the current assembly if any input field is null.
    def filter_null(*input_fields)
      each(input_fields, :filter => Java::CascadingOperationFilter::FilterNull.new)
    end
    alias reject_null filter_null

    # Rejects tuples from the current assembly if any input field is not null.
    def filter_not_null(*input_fields)
      each(input_fields, :filter => Java::CascadingOperationFilter::FilterNotNull.new)
    end
    alias where_null filter_not_null
  end
end
