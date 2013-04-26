module Cascading
  # Scope is a wrapper for a the private Cascading c.f.p.Scope object used to
  # connect the dataflow graph by resolving fields.  cascading.jruby wraps this
  # facility so that it may be used to propagate field names at composition
  # time (not Cascading plan time) in the same way they will later be
  # propagated by the planner.
  class Scope
    attr_accessor :scope

    # Construct a Scope given the Cascading c.f.p.Scope to wrap.
    def initialize(scope)
      @scope = scope
    end

    # Copy one Scope into another; relies upon the copy constructor of
    # c.f.p.Scope.
    def copy
      Scope.new(Java::CascadingFlowPlanner::Scope.new(@scope))
    end

    # Build a c.f.p.Scope for a Flow, which is empty except for its name.
    def self.flow_scope(name)
      Java::CascadingFlowPlanner::Scope.new(name)
    end

    # Build an empty Scope, wrapping an empty c.f.p.Scope.
    def self.empty_scope(name)
      Scope.new(Java::CascadingFlowPlanner::Scope.new(name))
    end

    # Build a Scope for a single source Tap.  The flow_scope is propagated
    # through this call into a new Scope.
    def self.source_scope(name, tap, flow_scope)
      incoming_scopes = java.util.HashSet.new
      incoming_scopes.add(flow_scope)
      java_scope = outgoing_scope_for(tap, incoming_scopes)
      # Taps and Pipes don't name their outgoing scopes like other FlowElements
      java_scope.name = name
      Scope.new(java_scope)
    end

    # Build a Scope for an arbitrary flow element.  This is used to update the
    # Scope at each stage in a pipe Assembly.
    def self.outgoing_scope(flow_element, incoming_scopes)
      java_scopes = incoming_scopes.compact.map{ |s| s.scope }
      Scope.new(outgoing_scope_for(flow_element, java.util.HashSet.new(java_scopes)))
    end

    # The values fields of the Scope, which indicate the fields in the current
    # dataflow tuple.
    def values_fields
      @scope.out_values_fields
    end

    # The grouping fields of the Scope, which indicate the keys of an
    # group/cogroup.
    def grouping_fields
      @scope.out_grouping_fields
    end

    # Prints a detailed description of this Scope, including its type and
    # various selectors, fields, and key fields.  Data is bubbled up directly
    # from the Cascading c.f.p.Scope.  This output can be useful for debugging
    # the propagation of fields through your job (see Flow#debug_scope and
    # Assembly#debug_scope, which both rely upon this method).
    def to_s
      kind = 'Unknown'
      kind = 'Tap'   if @scope.tap?
      kind = 'Group' if @scope.group?
      kind = 'Each'  if @scope.each?
      kind = 'Every' if @scope.every?
      <<-END
Scope name: #{@scope.name}
  Kind: #{kind}
  Key selectors:     #{scope_fields_to_s(:key_selectors)}
  Sorting selectors: #{scope_fields_to_s(:sorting_selectors)}
  Remainder fields:  #{scope_fields_to_s(:remainder_fields)}
  Declared fields:   #{scope_fields_to_s(:declared_fields)}
  Arguments
    selector:   #{scope_fields_to_s(:arguments_selector)}
    declarator: #{scope_fields_to_s(:arguments_declarator)}
  Out grouping
    selector:   #{scope_fields_to_s(:out_grouping_selector)}
    fields:     #{scope_fields_to_s(:out_grouping_fields)}
    key fields: #{scope_fields_to_s(:key_selectors)}
  Out values
    selector: #{scope_fields_to_s(:out_values_selector)}
    fields:   #{scope_fields_to_s(:out_values_fields)}
END
    end

    private

    def scope_fields_to_s(accessor)
      begin
        fields = @scope.send(accessor)
        fields.nil? ? 'null' : fields.to_s
      rescue Exception => e
        'ERROR'
      end
    end

    def self.outgoing_scope_for(flow_element, incoming_scopes)
      begin
        flow_element.outgoing_scope_for(incoming_scopes)
      rescue NativeException => e
        raise CascadingException.new(e, 'Exception computing outgoing scope')
      end
    end
  end
end
