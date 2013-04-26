require 'cascading/scope'

module Cascading
  # Allows you to plugin c.p.SubAssemblies to a cascading.jruby Assembly.
  #
  # Assumptions:
  # * You will either use the tail_pipe of the calling Assembly, or overwrite its incoming_scopes (as do join and union)
  # * Your subassembly will have only 1 tail pipe; branching is not supported.  This allows you to continue operating upon the tail of the SubAssembly within the calling Assembly
  # * You will not use nested c.p.SubAssemblies
  #
  # This is a low-level tool, so be careful.
  class SubAssembly
    attr_reader :assembly, :sub_assembly, :tail_pipe, :scope

    # Construct a SubAssembly within the given Assembly
    def initialize(assembly, sub_assembly)
      @assembly = assembly
      @sub_assembly = sub_assembly
      @tail_pipe = assembly.tail_pipe
      @scope = assembly.scope

      # Enforces 1 tail pipe assumption
      raise 'SubAssembly must call setTails in constructor' unless sub_assembly.tails
      raise 'SubAssembly must set exactly 1 tail in constructor' unless sub_assembly.tails.size == 1
    end

    # Complete the addition of the SubAssembly to the Assembly.  Propagates
    # Scope through the SubAssembly and updates the tail_pipe of the
    # SubAssembly for passing back to the enclosing Assembly.  May accept many
    # incoming pipes, but typically only recieves the tail_pipe of the
    # enclosing Assembly.
    def finalize(pipes, incoming_scopes)
      # Build adjacency list for sub_assembly
      graph = {}
      adjacency_list(pipes, sub_assembly.tails.first, graph)

      # Group adjacency list by next_pipe
      incoming_edges = graph.inject({}) do |incoming_edges, (prev_pipe, next_pipe)|
        incoming_edges[next_pipe] ||= []
        incoming_edges[next_pipe] << prev_pipe
        incoming_edges
      end

      # Propagate scope through sub_assembly graph
      inputs = Hash[*pipes.zip(incoming_scopes).flatten]
      while !incoming_edges.empty?
        incoming_edges.each do |next_pipe, prev_pipes|
          if (prev_pipes - inputs.keys).empty?
            input_scopes = prev_pipes.inject([]) do |input_scopes, prev_pipe|
              input_scopes << inputs.delete(prev_pipe)
              input_scopes
            end
            inputs[next_pipe] = Scope.outgoing_scope(next_pipe, input_scopes)
            incoming_edges.delete(next_pipe)
          end
        end
      end

      raise "Incoming edges did not capture all inputs; #{inputs.size} remaining" unless inputs.size == 1
      @tail_pipe, @scope = inputs.first
      raise "Expected scope propagation to end with tail pipe; ended with '#{@tail_pipe}' instead" unless sub_assembly.tails.first == @tail_pipe

      # This is the same "fix" applied to our field name metadata after a
      # sequence of Everies in Aggregations.  It just so happens that all of
      # CountBy, SumBy, and AverageBy end with Everies.  However, it appears to
      # only be necessary for AverageBy (which has different declaredFields for
      # its partials than its final, unlike the other two).  It would be nice
      # to track this issue down so that we can remove this hack from here and
      # Aggregations#finalize.
      discard_each = Java::CascadingPipe::Each.new(tail_pipe, all_fields, Java::CascadingOperation::Identity.new)
      @scope = Scope.outgoing_scope(discard_each, [scope])

      [@tail_pipe, @scope]
    end

    private

    def adjacency_list(pipes, tail_pipe, graph)
      unwound = Java::CascadingPipe::SubAssembly.unwind(tail_pipe).to_a
      # Join used because: http://jira.codehaus.org/browse/JRUBY-5136
      raise "SubAssembly does not support nested SubAssemblies; found #{unwound.size}: [#{unwound.join(',')}]" unless unwound.size == 1
      unwound = unwound.first

      unwound.previous.each do |pipe|
        raise 'SubAssembly does not support branching' if graph[pipe]
        graph[pipe] = tail_pipe

        if pipes.include?(pipe)
          next
        else
          adjacency_list(pipes, pipe, graph)
        end
      end
    end
  end
end
