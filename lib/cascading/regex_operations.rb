module Cascading
  # Module of pipe assemblies that wrap operations defined in the Cascading
  # cascading.operations.regex package.  These are split out only to group
  # similar functionality.
  #
  # All DSL regex pipes require an input_field, a regex, and either a single
  # into_field or one or more into_fields.  Requiring a single input field
  # allows us to raise an exception early if the wrong input is specified and
  # avoids the non-intuitive situation where the first of many fields is
  # silently taken as in Cascading.  Requiring a regex means you don't have to
  # go looking for defaults in code.  And into_field(s) means we can propagate
  # field names through the dataflow.
  #
  # Mapping of DSL pipes into Cascading regex operations:
  # parse:: {RegexParser}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/regex/RegexParser.html]
  # split:: {RegexSplitter}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/regex/RegexSplitter.html]
  # split\_rows:: {RegexSplitGenerator}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/regex/RegexSplitGenerator.html]
  # match\_rows:: {RegexGenerator}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/regex/RegexGenerator.html]
  # replace:: {RegexReplace}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/regex/RegexReplace.html]
  module RegexOperations
    # Parses the given input_field using the specified regular expression to
    # produce one output per group in that expression.
    #
    # The named params are:
    # [groups] Array of integers specifying which groups to capture if you want
    #          a subset of groups.
    #
    # Example:
    #     parse 'field1', /(\w+)\s+(\w+)/, ['out1', 'out2'], :groups => [1, 2]
    def parse(input_field, regex, into_fields, params = {})
      groups = params[:groups].to_java(:int) if params[:groups]
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1

      parameters = [fields(into_fields), regex.to_s, groups].compact
      each(
        input_field,
        :function => Java::CascadingOperationRegex::RegexParser.new(*parameters),
        :output => output
      )
    end

    # Splits the given input_field into multiple fields using the specified
    # regular expression.
    #
    # Example:
    #     split 'line', /\s+/, ['out1', 'out2']
    def split(input_field, regex, into_fields, params = {})
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1

      each(
        input_field,
        :function => Java::CascadingOperationRegex::RegexSplitter.new(fields(into_fields), regex.to_s),
        :output => output
      )
    end

    # Splits the given input_field into new rows using the specified regular
    # expression.
    #
    # Example:
    #     split_rows 'line', /\s+/, 'word'
    def split_rows(input_field, regex, into_field, params = {})
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1
      into_field = fields(into_field)
      raise "into_field must declare exactly one field, was '#{into_field}'" unless into_field.size == 1

      each(
        input_field,
        :function => Java::CascadingOperationRegex::RegexSplitGenerator.new(into_field, regex.to_s),
        :output => output
      )
    end

    # Emits a new row for each regex group matched in input_field using the
    # specified regular expression.
    #
    # Example:
    #     match_rows 'line', /(\w+)\s+(\w+)/, 'word'
    def match_rows(input_field, regex, into_field, params = {})
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1
      into_field = fields(into_field)
      raise "into_field must declare exactly one field, was '#{into_field}'" unless into_field.size == 1

      each(
        input_field,
        :function => Java::CascadingOperationRegex::RegexGenerator.new(into_field, regex.to_s),
        :output => output
      )
    end

    # Performs a query/replace on the given input_field using the specified
    # regular expression and replacement.
    #
    # The named params are:
    # [replace_all] Boolean indicating if all matches should be replaced;
    #               defaults to true (the Cascading default).
    #
    # Example:
    #     replace 'line', /[.,]*\s+/, 'tab_separated_line', "\t"
    def replace(input_field, regex, into_field, replacement, params = {})
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1
      into_field = fields(into_field)
      raise "into_field must declare exactly one field, was '#{into_field}'" unless into_field.size == 1

      parameters = [into_field, regex.to_s, replacement.to_s, params[:replace_all]].compact
      each(
        input_field,
        :function => Java::CascadingOperationRegex::RegexReplace.new(*parameters),
        :output => output
      )
    end
  end
end
