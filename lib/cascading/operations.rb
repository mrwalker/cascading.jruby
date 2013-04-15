module Cascading
  # The Cascading::Operations module is deprecated.  The original idea from long
  # ago is that it would be useful to mixin operator wrappers to places other
  # than Cascading::Assembly, but this is not true.  Instead, put Eaches in
  # Cascading::Assembly, Everies in Cascading::Aggregations, and any more
  # generally useful utility code directly in the Cascading module
  # (cascading/cascading.rb).
  #
  # Further, the entire *args pattern should be deprecated as it leads to
  # functions that can only be understood by reading their code.  Instead,
  # idiomatic Ruby (positional required params and a params hash for optional
  # args) should be used.  See Cascading::Assembly#set_value for an example.
  module Operations
    def identity
      Java::CascadingOperation::Identity.new
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

    def expression_function(*args)
      options = args.extract_options!

      fields = Cascading.fields(args)
      expression = options[:expression].to_s
      parameters = options[:parameters]
      parameter_names = []
      parameter_types = []
      if parameters.is_a? ::Hash
        parameters.each do |name, type|
          parameter_names << name
          parameter_types << type
        end
        parameter_names = parameter_names.to_java(java.lang.String)
        parameter_types = parameter_types.to_java(java.lang.Class)

        arguments = [fields, expression, parameter_names, parameter_types].compact
      elsif !parameters.nil?
        arguments = [fields, expression, parameters.java_class].compact
      else
        arguments = [fields, expression, java.lang.String.java_class].compact
      end

      Java::CascadingOperationExpression::ExpressionFunction.new(*arguments)
    end

    def insert_function(*args)
      options=args.extract_options!
      fields = Cascading.fields(args)
      values = options[:values]

      parameters = [fields, to_java_comparable_array(values)].compact
      Java::CascadingOperation::Insert.new(*parameters)
    end

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

    def expression_filter(*args)
      options = args.extract_options!
      expression = (args[0] || options[:expression]).to_s
      parameters = options[:parameters]
      parameter_names = []
      parameter_types = []
      if parameters.is_a? ::Hash
        parameters.each do |name, type|
          parameter_names << name
          parameter_types << type
        end
        parameter_names = parameter_names.to_java(java.lang.String)
        parameter_types = parameter_types.to_java(java.lang.Class)

        arguments = [expression, parameter_names, parameter_types].compact
      elsif !parameters.nil?
        arguments = [expression, parameters.java_class].compact
      else
        arguments = [expression, java.lang.String.java_class].compact
      end

      Java::CascadingOperationExpression::ExpressionFilter.new(*arguments)
    end

    def date_parser(field, format)
      fields = fields(field)
      Java::CascadingOperationText::DateParser.new(fields, format)
    end

    def date_formatter(fields, format, timezone=nil)
      fields = fields(fields)
      timezone = Java::JavaUtil::TimeZone.get_time_zone(timezone) if timezone
      arguments = [fields, format, timezone].compact
      Java::CascadingOperationText::DateFormatter.new(*arguments)
    end

    def regex_filter(*args)
      options = args.extract_options!

      pattern = args[0]
      remove_match = options[:remove_match]
      match_each_element = options[:match_each_element]
      parameters = [pattern.to_s, remove_match, match_each_element].compact
      Java::CascadingOperationRegex::RegexFilter.new(*parameters)
    end

    def field_joiner(*args)
      options = args.extract_options!
      delimiter = options[:delimiter] || ','
      fields = fields(options[:into])

      parameters = [fields, delimiter].compact
      Java::CascadingOperationText::FieldJoiner.new(*parameters)
    end
  end
end
