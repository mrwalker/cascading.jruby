module Cascading
  # Module of pipe assemblies that wrap the Cascading Identity operation.  These
  # are split out only to group similar functionality.
  module IdentityOperations
    # Restricts the current assembly to the specified fields in the order in
    # which they are specified (can be used to reorder fields).
    #
    # Example:
    #     project 'field1', 'field2'
    def project(*input_fields)
      each fields(input_fields), :function => Java::CascadingOperation::Identity.new
    end

    # Removes the specified fields from the current assembly.
    #
    # Example:
    #     discard 'field1', 'field2'
    def discard(*input_fields)
      discard_fields = fields(input_fields)
      keep_fields = difference_fields(scope.values_fields, discard_fields)
      project(*keep_fields.to_a)
    end

    # Renames fields according to the mapping provided, preserving the original
    # field order.  Throws an exception if non-existent fields are specified.
    # 
    # Example:
    #     rename 'field1' => 'fieldA', 'field2' => 'fieldB'
    #
    # Produces: ['fieldA', 'fieldB'], assuming those were the only 2 input
    # fields.
    def rename(name_map)
      original_fields = scope.values_fields.to_a
      invalid = name_map.keys - original_fields
      raise "Invalid field names in rename: #{invalid.inspect}" unless invalid.empty?

      renamed_fields = original_fields.map{ |name| name_map[name] || name }

      each original_fields, :function => Java::CascadingOperation::Identity.new(fields(renamed_fields))
    end

    # Coerces fields to the Java type selected from Cascading::JAVA_TYPE_MAP.
    #
    # Example:
    #     cast 'field1' => :int, 'field2' => :double
    def cast(type_map)
      input_fields = type_map.keys.sort
      types = JAVA_TYPE_MAP.values_at(*type_map.values_at(*input_fields))
      input_fields = fields(input_fields)
      types = types.to_java(java.lang.Class)
      each input_fields, :function => Java::CascadingOperation::Identity.new(input_fields, types)
    end

    # A field copy (not a pipe copy).  Renames fields according to name_map,
    # appending them to the fields in the assembly in the same order as the
    # original fields from which they are copied.  Throws an exception if
    # non-existent fields are specified.
    #
    # Example:
    #     copy 'field1' => 'fieldA', 'field2' => 'fieldB'
    #
    # Produces: ['field1', 'field2', 'fieldA', 'fieldB'], assuming those were
    # the only input fields.
    def copy(name_map)
      original_fields = scope.values_fields.to_a
      invalid = name_map.keys - original_fields
      raise "Invalid field names in copy: #{invalid.inspect}" unless invalid.empty?

      # Original fields in name_map in their original order
      input_fields = original_fields - (original_fields - name_map.keys)
      into_fields = name_map.values_at(*input_fields)

      each input_fields, :function => Java::CascadingOperation::Identity.new(fields(into_fields)), :output => all_fields
    end

    # A pipe copy (not a field copy).  Can be used within a branch to copy a
    # pipe.
    def pass
      each all_fields, :function => Java::CascadingOperation::Identity.new
    end
  end
end
