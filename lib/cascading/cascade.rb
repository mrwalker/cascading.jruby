require 'cascading/base'
require 'yaml'

module Cascading
  class Cascade < Cascading::Node
    extend Registerable

    attr_reader :properties, :mode

    # Do not use this constructor directly; instead, use Cascading::cascade to
    # build cascades.
    #
    # Builds a cascade given the specified name.  Optionally accepts
    # :properties which will be used as the default properties for all child
    # flows.  Properties must be a Ruby Hash with string keys and values and
    # will be copied before being passed into each flow in the cascade.  See
    # Cascading::Flow#initialize for details on how flows handle properties.
    # Optionally accepts a :mode which will be used as the default mode for all
    # child flows.  See Cascading::Mode.parse for details.
    def initialize(name, options = {})
      @properties = options[:properties] || {}
      @mode = options[:mode]
      super(name, nil) # A Cascade cannot have a parent
      self.class.add(name, self)
    end

    # Builds a child flow given a name and block.  Optionally accepts
    # :properties which will override the default properties stroed in this
    # cascade.  Optionally accepts a :mode, which will override the default
    # mode stored in this cascade.
    def flow(name, options = {}, &block)
      raise "Could not build flow '#{name}'; block required" unless block_given?

      options[:properties] ||= properties.dup
      options[:mode] ||= mode

      flow = Flow.new(name, self, options)
      add_child(flow)
      flow.instance_eval(&block)
      flow
    end

    def describe(offset = '')
      "#{offset}#{name}:cascade\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}"
    end

    def draw(dir)
      @children.each do |name, flow|
        flow.connect.writeDOT("#{dir}/#{name}.dot")
      end
    end

    def sink_metadata
      @children.inject({}) do |sink_fields, (name, flow)|
        sink_fields[name] = flow.sink_metadata
        sink_fields
      end
    end

    def write_sink_metadata(file_name)
      File.open(file_name, 'w') do |file|
        YAML.dump(sink_metadata, file)
      end
    end

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
