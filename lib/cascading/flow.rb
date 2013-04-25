require 'cascading/assembly'

module Cascading
  # A Flow wraps a c.f.Flow.  A Flow is composed of Assemblies, which are
  # constructed using the Flow#assembly method within the block passed to the
  # Cascading::flow or Cascade#flow constructor.  Many Assemblies may be nested
  # within a Flow.
  class Flow < Cascading::Node
    extend Registerable

    attr_accessor :sources, :sinks, :incoming_scopes, :outgoing_scopes, :listeners
    attr_reader :properties, :mode

    # Do not use this constructor directly.  Instead, use Cascading::flow to
    # build top-level flows and Cascade#flow to build flows within a Cascade.
    #
    # Builds a Flow given a name and a parent node (a Cascade or nil).
    #
    # The named options are:
    # [properties] Properties hash which allows external configuration of this
    #              flow.  The flow will side-effect the properties during
    #              composition, then pass the modified properties along to the
    #              FlowConnector for execution.  See Cascade#initialize for
    #              details on how properties are propagated through cascades.
    # [mode] Mode which will determine the execution mode of this flow.  See
    #        Mode.parse for details.
    def initialize(name, parent, options = {})
      @sources, @sinks, @incoming_scopes, @outgoing_scopes, @listeners = {}, {}, {}, {}, []
      @properties = options[:properties] || {}
      @mode = Mode.parse(options[:mode])
      @flow_scope = Scope.flow_scope(name)
      super(name, parent)
      self.class.add(name, self)
    end

    # Builds a child Assembly in this Flow given a name and block.
    #
    # An assembly's name is quite important as it will determine:
    # * The sources from which it will read, if any
    # * The name to be used in joins or unions downstream
    # * The name to be used to sink the output of the assembly downstream
    #
    # Many assemblies may be built within a flow.  The Assembly#branch method
    # is used for creating nested assemblies and produces objects of the same
    # type as this constructor.
    #
    # Example:
    #     flow 'wordcount', :mode => :local do
    #       assembly 'first_step' do
    #         ...
    #       end
    #
    #       assembly 'second_step' do
    #         ...
    #       end
    #     end
    def assembly(name, &block)
      raise "Could not build assembly '#{name}'; block required" unless block_given?
      assembly = Assembly.new(name, self, @outgoing_scopes)
      add_child(assembly)
      assembly.instance_eval(&block)
      assembly
    end

    # Create a new source for this flow, using the specified name and
    # Cascading::Tap
    def source(name, tap)
      sources[name] = tap
      incoming_scopes[name] = Scope.source_scope(name, mode.source_tap(name, tap), @flow_scope)
      outgoing_scopes[name] = incoming_scopes[name]
    end

    # Create a new sink for this flow, using the specified name and
    # Cascading::Tap
    def sink(name, tap)
      sinks[name] = tap
    end

    # Produces a textual description of this Flow.  The description details the
    # structure of the Flow, its sources and sinks, and the input and output
    # fields of each Assembly.  The offset parameter allows for this describe
    # to be nested within a calling context, which lets us indent the
    # structural hierarchy of a job.
    def describe(offset = '')
      description =  "#{offset}#{name}:flow\n"
      description += "#{sources.keys.map{ |source| "#{offset}  #{source}:source :: #{incoming_scopes[source].values_fields.to_a.inspect}" }.join("\n")}\n"
      description += "#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}\n"
      description += "#{sinks.keys.map{ |sink| "#{offset}  #{sink}:sink :: #{outgoing_scopes[sink].values_fields.to_a.inspect}" }.join("\n")}"
      description
    end

    # Accesses the outgoing scope of this Flow at the point at which it is
    # called by default, or for the child specified by the given name, if
    # specified.  This is useful for grabbing the values_fields at any point in
    # the construction of the Flow.  See Scope for details.
    def scope(name = nil)
      raise 'Must specify name if no children have been defined yet' unless name || last_child
      name ||= last_child.name
      @outgoing_scopes[name]
    end

    # Prints information about the scope of this Flow at the point at which it
    # is called by default, or for the child specified by the given name, if
    # specified.  This allows you to trace the propagation of field names
    # through your job and is handy for debugging.  See Scope for details.
    def debug_scope(name = nil)
      scope = scope(name)
      name ||= last_child.name
      puts "Scope for '#{name}':\n  #{scope}"
    end

    # Builds a map, keyed by sink name, of the sink metadata for each sink.
    # Currently, this contains only the field names of each sink.
    def sink_metadata
      @sinks.keys.inject({}) do |sink_metadata, sink_name|
        raise "Cannot sink undefined assembly '#{sink_name}'" unless @outgoing_scopes[sink_name]
        sink_metadata[sink_name] = {
          :field_names => @outgoing_scopes[sink_name].values_fields.to_a,
        }
        sink_metadata
      end
    end

    # Property modifier that sets the codec and type of the compression for all
    # sinks in this flow.  Currently only supports o.a.h.i.c.DefaultCodec and
    # o.a.h.i.c.GzipCodec, and the the NONE, RECORD, or BLOCK compressions
    # types defined in o.a.h.i.SequenceFile.
    #
    # codec may be symbols like :default or :gzip and type may be symbols like
    # :none, :record, or :block.
    #
    # Example:
    #     compress_output :default, :block
    def compress_output(codec, type)
      properties['mapred.output.compress'] = 'true'
      properties['mapred.output.compression.codec'] = case codec
        when :default then Java::OrgApacheHadoopIoCompress::DefaultCodec.java_class.name
        when :gzip then Java::OrgApacheHadoopIoCompress::GzipCodec.java_class.name
        else raise "Codec #{codec} not yet supported by cascading.jruby"
        end
      properties['mapred.output.compression.type'] = case type
        when :none   then Java::OrgApacheHadoopIo::SequenceFile::CompressionType::NONE.to_s
        when :record then Java::OrgApacheHadoopIo::SequenceFile::CompressionType::RECORD.to_s
        when :block  then Java::OrgApacheHadoopIo::SequenceFile::CompressionType::BLOCK.to_s
        else raise "Compression type '#{type}' not supported"
        end
    end

    # Set the cascading.spill.list.threshold property in this flow's
    # properties.  See c.t.c.SpillableProps for details.
    def set_spill_threshold(threshold)
      properties['cascading.spill.list.threshold'] = threshold.to_s
    end

    # Adds the given path to the mapred.cache.files list property.
    def add_file_to_distributed_cache(file)
      add_to_distributed_cache(file, "mapred.cache.files")
    end

    # Adds the given path to the mapred.cache.archives list property.
    def add_archive_to_distributed_cache(file)
      add_to_distributed_cache(file, "mapred.cache.archives")
    end

    # Appends a FlowListener to the list of listeners for this flow.
    def add_listener(listener)
      @listeners << listener
    end

    # Handles locating a file cached from S3 on local disk.  TODO: remove
    def emr_local_path_for_distributed_cache_file(file)
      # NOTE this needs to be *appended* to the property mapred.local.dir
      if file =~ /^s3n?:\/\//
        # EMR
        "/taskTracker/archive/#{file.gsub(/^s3n?:\/\//, "")}"
      else
        # Local
        file
      end
    end

    # Connects this Flow, producing a c.f.Flow without completing it (the Flow
    # is not executed).  This method is used by Cascade to connect its child
    # Flows.  To connect and complete a Flow, see Flow#complete.
    def connect
      puts "Connecting flow '#{name}' with properties:"
      properties.keys.sort.each do |key|
        puts "#{key}=#{properties[key]}"
      end

      # FIXME: why do I have to do this in 2.0 wip-255?
      Java::CascadingProperty::AppProps.setApplicationName(properties, name)
      Java::CascadingProperty::AppProps.setApplicationVersion(properties, '0.0.0')
      Java::CascadingProperty::AppProps.setApplicationJarClass(properties, Java::CascadingFlow::Flow.java_class)

      sources = make_tap_parameter(@sources, :head_pipe)
      sinks = make_tap_parameter(@sinks, :tail_pipe)
      pipes = make_pipes
      mode.connect_flow(properties, name, sources, sinks, pipes)
    end

    # Completes this Flow after connecting it.  This results in execution of
    # the c.f.Flow built from this Flow.  Use this method when executing a
    # top-level Flow.
    def complete
      begin
        flow = connect
        @listeners.each { |l| flow.addListener(l) }
        flow.complete
      rescue NativeException => e
        raise CascadingException.new(e, 'Error completing flow')
      end
    end

    private

    def add_to_distributed_cache(file, property)
      v = properties[property]

      if v
        properties[property] = [v.split(/,/), file].flatten.join(",")
      else
        properties[property] = file
      end
    end

    def make_tap_parameter(taps, pipe_accessor)
      taps.inject({}) do |map, (name, tap)|
        assembly = find_child(name)
        raise "Could not find assembly '#{name}' to connect to tap: #{tap}" unless assembly
        map[assembly.send(pipe_accessor).name] = tap
        map
      end
    end

    def make_pipes
      @sinks.inject([]) do |pipes, (name, sink)|
        assembly = find_child(name)
        raise "Could not find assembly '#{name}' to make pipe for sink: #{sink}" unless assembly
        pipes << assembly.tail_pipe
        pipes
      end.to_java(Java::CascadingPipe::Pipe)
    end
  end
end
