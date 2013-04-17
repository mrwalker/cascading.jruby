module Cascading
  # Module of pipe assemblies that wrap operations defined in the Cascading
  # cascading.operations.text package.  These are split out only to group
  # similar functionality.
  #
  # Mapping of DSL pipes into Cascading text operations:
  # parse\_date:: {DateParser}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/text/DateParser.html]
  # format\_date:: {DateFormatter}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/text/DateFormatter.html]
  # join\_fields:: {FieldJoiner}[http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/text/FieldJoiner.html]
  module TextOperations
    # Parses the given input_field as a date using the provided format string.
    #
    # Example:
    #     parse_date 'text_date', 'yyyy/MM/dd', 'timestamp'
    def parse_date(input_field, date_format, into_field, params = {})
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1
      into_field = fields(into_field)
      raise "into_field must declare exactly one field, was '#{into_field}'" unless into_field.size == 1

      each(
        input_field,
        :function => Java::CascadingOperationText::DateParser.new(into_field, date_format),
        :output => output
      )
    end

    # Converts a timestamp into a formatted date string using the specified
    # date_format.
    #
    # Example:
    #     format_date 'timestamp', 'yyyy/MM/dd', 'text_date'
    def format_date(input_field, date_format, into_field, params = {})
      output = params[:output] || all_fields # Overrides Cascading default

      input_field = fields(input_field)
      raise "input_field must declare exactly one field, was '#{input_field}'" unless input_field.size == 1
      into_field = fields(into_field)
      raise "into_field must declare exactly one field, was '#{into_field}'" unless into_field.size == 1

      each(
        input_field,
        :function => Java::CascadingOperationText::DateFormatter.new(into_field, date_format),
        :output => output
      )
    end

    # Joins multiple fields into a single field given a delimiter.
    #
    # Example:
    #     join_fields ['field1', 'field2'], ',', 'comma_separated'
    def join_fields(input_fields, delimiter, into_field)
      output = params[:output] || all_fields # Overrides Cascading default

      into_field = fields(into_field)
      raise "into_field must declare exactly one field, was '#{into_field}'" unless into_field.size == 1

      each(
        input_fields,
        :function => Java::CascadingOperationText::FieldJoiner.new(into_field, delimiter.to_s),
        :output => output
      )
    end
  end
end
