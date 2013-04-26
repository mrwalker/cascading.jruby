module Cascading
  # A Mode encapsulates the idea of the execution mode for your flows.  The
  # default is Hadoop mode, but you can request that your code run in Cascading
  # local mode.  If you subsequently use a tap or a scheme that has no local
  # implementation, the mode will be converted back to Hadoop mode.
  class Mode
    attr_reader :local

    # Parses a specification of which mode, Cascading local mode or Hadoop mode,
    # to execute in.  Defaults to Hadoop mode.  You may explicitly request
    # Cascading local mode with values 'local' or :local.  If you pass a Mode
    # object to this method, it will be passed through.
    def self.parse(mode)
      case mode
      when Mode then mode
      when 'local', :local then Mode.new(true)
      else Mode.new(false)
      end
    end

    # Constructs a Mode given a flag indicating if it should be Cascading local
    # mode.
    def initialize(local)
      @local = local
    end

    # Attempts to select the appropriate tap given the current mode.  If that
    # tap does not exist, it fails over to the other tap with a warning.
    def source_tap(name, tap)
      warn "WARNING: No local tap for source '#{name}' in tap #{tap}" if local && !tap.local?
      warn "WARNING: No Hadoop tap for source '#{name}' in tap #{tap}" if !local && !tap.hadoop?

      if local
        tap.local_tap || tap.hadoop_tap
      else
        tap.hadoop_tap || tap.local_tap
      end
    end

    # Builds a c.f.Flow given properties, name, sources, sinks, and pipes from
    # a Flow.  The current mode is adjusted based on the taps and schemes of
    # the sources and sinks, then the correct taps are selected before building
    # the flow.
    def connect_flow(properties, name, sources, sinks, pipes)
      update_local_mode(sources, sinks)
      sources = select_taps(sources)
      sinks = select_taps(sinks)

      # Report execution mode to stdout before connecting
      puts "Connecting flow '#{name}' in #{local ? 'Cascading local mode' : 'Hadoop mode'}"

      flow_connector_class.new(java.util.HashMap.new(properties)).connect(name, sources, sinks, pipes)
    end

    private

    # Updates this mode based upon your sources and sinks.  It's possible that
    # you asked for Cascading local mode, but that request cannot be fulfilled
    # because you used taps or schemes which have no local implementation.
    def update_local_mode(sources, sinks)
      local_supported = sources.all?{ |name, tap| tap.local? } && sinks.all?{ |name, tap| tap.local? }

      if local && !local_supported
        non_local_sources = sources.reject{ |name, tap| tap.local? }
        non_local_sinks = sinks.reject{ |name, tap| tap.local? }
        warn "WARNING: Cascading local mode requested but these sources: #{non_local_sources.inspect} and these sinks: #{non_local_sinks.inspect} do not support it"
        @local = false
      end

      local
    end

    # Given a tap map, extracts the correct taps for the current mode
    def select_taps(tap_map)
      tap_map.inject({}) do |map, (name, tap)|
        map[name] = tap.send(local ? :local_tap : :hadoop_tap)
        map
      end
    end

    # Chooses the correct FlowConnector class for the current mode
    def flow_connector_class
      local ? Java::CascadingFlowLocal::LocalFlowConnector : Java::CascadingFlowHadoop::HadoopFlowConnector
    end
  end
end
