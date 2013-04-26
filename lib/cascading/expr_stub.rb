module Cascading
  class ExprStub
    attr_accessor :expression, :types, :input_expression

    # ExprStub requires a Janino expression decorated with field types.  For
    # example:
    #     expr('"Found: " + (x:int + y:int) + " " + z:string')
    # Type names are defined in Cascading::JAVA_TYPE_MAP.
    def initialize(expression)
      @input_expression = expression
      @expression = expression.dup
      @types = {}

      # Simple regexp based parser for types
      JAVA_TYPE_MAP.each do |sym, klass|
        @expression.gsub!(/[A-Za-z0-9_]+:#{sym.to_s}/) do |match|
          name = match.split(/:/).first.gsub(/\s+/, "")
          @types[name] = klass
          match.gsub(/:#{sym.to_s}/, "")
        end
      end
    end

    # Extract Java names and types from @types hash.  Cascading constructors
    # often require two separate Java Arrays in this fashion.
    def names_and_types
      names, types = split_hash(@types)
      [names.to_java(java.lang.String), types.to_java(java.lang.Class)]
    end

    # Prints the original input expression.
    def to_s
      @input_expression
    end

    # Convenience constructor for an ExprStub that optionally performs
    # validation.  Takes a string to use as a Janino expression and an optional
    # options hash.
    #
    # The named options are:
    # [validate] A boolean indicating whether expression validation using
    #            default actual argument values should be performed.  Defaults
    #            to true.
    # [validate_with] A hash mapping field names (or symbols) to the value that
    #                 should be used for validation.  Strings default to nil,
    #                 so if you have previously filtered nulls you might use a
    #                 marker value like 'nulls_filtered'.  Defaults to {}.
    #
    # Example:
    #     insert 'x_eq_y' => expr('x:string.equals(y:string)', :validate_with => { :x => 'nulls_filtered' })
    def self.expr(expression, options = {})
      options = { :validate => true, :validate_with => {} }.merge(options)
      expr_stub = expression.kind_of?(ExprStub) ? expression : ExprStub.new(expression).compile
      expr_stub.validate(options[:validate_with]) if options[:validate]
      puts "Expression validation is disabled for '#{expression}'" unless options[:validate]
      expr_stub
    end

    # Scan, parse, and compile expression, then return this ExprStub upon
    # success.  Throws an CascadingException upon failure.
    def compile
      evaluator
      self
    end

    # Evaluates this ExprStub given a hash mapping argument names to argument
    # values. Names may be strings or symbols. Throws an CascadingException
    # upon failure.
    def eval(actual_args)
      actual_args = actual_args.inject({}) do |string_keys, (arg, value)|
        string_keys[arg.to_s] = specific_to_java(value, @types[arg.to_s])
        string_keys
      end
      args, values = split_hash(actual_args)
      unused = validate_fields(args)
      return self.eval(actual_args.reject{ |arg, value| unused.include?(arg) }) unless unused.empty?
      evaluate(values)
    end

    # Evaluates this ExprStub with default values for each actual argument.
    # Values may be overridden with the optional actual_args argument, which
    # accepts a hash like ExprStub#eval. Throws an CascadingException upon
    # failure.
    def validate(actual_args = {})
      self.eval(test_values.merge(actual_args))
    end

    # Given a scope, validates that the fields required by this ExprStub are
    # available in the values fields of the scope.  Returns those values fields
    # which are unused in the expression.
    def validate_scope(scope)
      validate_fields(scope.values_fields.to_a)
    end

    # Throws an exception if any arguments required by this ExprStub are
    # missing from fields.  Returns those fields which are unused. Throws an
    # ExprArgException upon failure.
    def validate_fields(fields)
      names = @types.keys.sort
      missing = names - fields
      raise ExprArgException.new("Expression '#{@expression}' is missing these fields: #{missing.inspect}\nRequires: #{names.inspect}, found: #{fields.inspect}") unless missing.empty?
      fields - names
    end

    private

    def split_hash(h)
      keys, values = h.sort.inject([[], []]) do |(keys, values), (key, value)|
        [keys << key, values << value]
      end
      [keys, values]
    end

    # Evaluate this ExprStub given an array of actual arguments. Throws an
    # CascadingException upon failure. GOTCHA: requires values to be in order
    # of lexicographically sorted formal arguments.
    def evaluate(values)
      begin
        evaluator.evaluate(values.to_java)
      rescue NativeException => ne
        raise CascadingException.new(ne, "Exception encountered while evaluating '#{@expression}' with arguments: #{values.inspect}")
      end
    end

    # Building an evaluator ensures that the expression scans, parses, and
    # compiles
    def evaluator
      begin
        names, types = names_and_types
        Java::OrgCodehausJanino::ExpressionEvaluator.new(@expression, java.lang.Comparable.java_class, names, types)
      rescue NativeException => ne
        raise CascadingException.new(ne, "Exception encountered while compiling '#{@expression}'")
      end
    end

    # Makes best effort to convert Ruby numbers into the Java numeric type
    # exepcted by a Janino expression. However, if the conversion fails, it
    # returns the original value so that the exception thrown will be from
    # Janino, not this code.
    def specific_to_java(value, type)
      # GOTCHA: Java's Float and Long have constructors that take strings and
      # parse them.  If value is a string representation of a number, this code
      # could coerce it to a number whereas invocation of the Janino expression
      # would fail.  We therefore punt if value is a String.
      return value if value.kind_of?(::String)
      if type == java.lang.Float.java_class
        return value if value.kind_of?(::Integer)
        java.lang.Float.new(value) rescue value
      elsif type == java.lang.Long.java_class && JRUBY_VERSION <= '1.2.0'
        return value if value.kind_of?(::Float)
        java.lang.Long.new(value) rescue value
      elsif type == java.lang.Integer.java_class && JRUBY_VERSION > '1.2.0'
        return value if value.kind_of?(::Float)
        java.lang.Integer.new(value) rescue value
      else
        value
      end
    end

    @@defaults = {
      java.lang.Integer.java_class => JRUBY_VERSION > '1.2.0' ? java.lang.Integer.new(0) : 0,
      java.lang.Boolean.java_class => false,
      java.lang.Double.java_class => 0.0,
      java.lang.Float.java_class => java.lang.Float.new(0.0),
      java.lang.Long.java_class => JRUBY_VERSION > '1.2.0' ? 0 : java.lang.Long.new(0),
      java.lang.String.java_class => nil,
    }

    def test_values
      @types.sort.inject({}) do |test_values, (name, type)|
        test_values[name] = @@defaults[type]
        test_values
      end
    end
  end

  class ExprArgException < StandardError; end
end
