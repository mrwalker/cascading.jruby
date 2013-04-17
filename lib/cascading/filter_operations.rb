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
  end
end
