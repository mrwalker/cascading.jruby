require 'cascading/base'
require 'yaml'

module Cascading
  # A Cascade wraps a c.c.Cascade.  A Cascade is composed of Flows, which are
  # constructed using the Cascade#flow method within the block passed to the
  # Cascading::cascade constructor.  Many flows may be nested within a Cascade.
  #
  # Note that you are not required to use a Cascade to wrap your job.  Instead,
  # you could start with a top-level Flow, which you might prefer if you have
  # no need of a c.c.Cascade's make-like semantics wrt sinks.
  class Cascade < Cascading::Node
    extend Registerable

    attr_reader :properties, :mode

    # Do not use this constructor directly; instead, use Cascading::cascade to
    # build cascades.
    #
    # Builds a Cascade given a name.
    #
    # The named options are:
    # [properties] Properties hash which will be used as the default properties
    #              for all child flows.  Properties must be a Ruby Hash with
    #              string keys and values and will be copied before being
    #              passed into each flow in the cascade.  See Flow#initialize
    #              for details on how flows handle properties.
    # [mode] Mode which will be used as the default mode for all child flows.
    #        See Mode.parse for details.
    def initialize(name, options = {})
      @properties = options[:properties] || {}
      @mode = options[:mode]
      super(name, nil) # A Cascade cannot have a parent
      self.class.add(name, self)
    end

    # Builds a child Flow in this Cascade given a name and block.
    #
    # The named options are:
    # [properties] Properties hash which will override the default properties
    #              stored in this cascade.
    # [mode] Mode which will override the default mode stored in this cascade.
    #
    # Example:
    #     cascade 'wordcount', :mode => :local do
    #       flow 'first_step' do
    #         ...
    #       end
    #
    #       flow 'second_step' do
    #         ...
    #       end
    #     end
    def flow(name, options = {}, &block)
      raise "Could not build flow '#{name}'; block required" unless block_given?

      options[:properties] ||= properties.dup
      options[:mode] ||= mode

      flow = Flow.new(name, self, options)
      add_child(flow)
      flow.instance_eval(&block)
      flow
    end

    # Produces a textual description of this Cascade.  The description details
    # the structure of the Cascade, the sources and sinks of each Flow, and the
    # input and output fields of each Assembly.  The offset parameter allows
    # for this describe to be nested within a calling context, which lets us
    # indent the structural hierarchy of a job.
    def describe(offset = '')
      "#{offset}#{name}:cascade\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}"
    end

    # Writes out the DOT file describing the structure of this Cascade.
    #
    # NOTE: will be at Job in later version and also present on Flow
    def draw(dir)
      @children.each do |name, flow|
        flow.connect.writeDOT("#{dir}/#{name}.dot")
      end
    end

    # Builds a map, keyed by flow name, of the sink metadata for each child
    # flow.  Currently, this contains only the field names of each sink.
    def sink_metadata
      @children.inject({}) do |sink_fields, (name, flow)|
        sink_fields[name] = flow.sink_metadata
        sink_fields
      end
    end

    # Writes the mapping produced by Cascade#sink_metadata to a file at the
    # given path in YAML.
    def write_sink_metadata(file_name)
      File.open(file_name, 'w') do |file|
        YAML.dump(sink_metadata, file)
      end
    end

    # Connects this Cascade, producing a c.c.Cascade, which is then completed,
    # executing it.  Child flows are connected, so no parameters are required.
    def complete
      begin
        Java::CascadingCascade::CascadeConnector.new.connect(name, make_flows(@children)).complete
      rescue NativeException => e
        raise CascadingException.new(e, 'Error completing cascade')
      end
    end

    private

    def make_flows(flows)
      flow_instances = flows.map do |name, flow|
        cascading_flow = flow.connect
        flow.listeners.each { |l| cascading_flow.addListener(l) }
        cascading_flow
      end
      flow_instances.to_java(Java::CascadingFlow::Flow)
    end
  end
end
