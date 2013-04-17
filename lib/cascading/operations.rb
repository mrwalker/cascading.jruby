module Cascading
  module Operations
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
  end
end
