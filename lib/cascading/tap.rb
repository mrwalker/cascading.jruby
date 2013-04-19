module Cascading
  # A Cascading::BaseTap wraps up a pair of Cascading taps, one for Cascading
  # local mode and the other for Hadoop mode.
  class BaseTap
    attr_reader :local_tap, :hadoop_tap

    def initialize(local_tap, hadoop_tap)
      @local_tap = local_tap
      @hadoop_tap = hadoop_tap
    end

    def to_s
      "Local: #{local_tap}, Hadoop: #{hadoop_tap}"
    end

    def local?
      !local_tap.nil?
    end

    def hadoop?
      !hadoop_tap.nil?
    end
  end

  # A Cascading::Tap represents a non-aggregate tap with a scheme, path, and
  # optional sink_mode.  c.t.l.FileTap is used in Cascading local mode and
  # c.t.h.Hfs is used in Hadoop mode.  Whether or not these can be created is
  # governed by the :scheme parameter, which must contain at least one of
  # :local_scheme or :hadoop_scheme.  Schemes like TextLine are supported in
  # both modes (by Cascading), but SequenceFile is only supported in Hadoop
  # mode.
  class Tap < BaseTap
    attr_reader :scheme, :path, :sink_mode

    def initialize(path, options = {})
      @path = path

      @scheme = options[:scheme] || text_line_scheme
      raise "Scheme must provide one of :local_scheme or :hadoop_scheme; received: '#{scheme.inspect}'" unless scheme[:local_scheme] || scheme[:hadoop_scheme]

      @sink_mode = case options[:sink_mode] || :keep
        when :keep, 'keep'       then Java::CascadingTap::SinkMode::KEEP
        when :replace, 'replace' then Java::CascadingTap::SinkMode::REPLACE
        when :append, 'append'   then Java::CascadingTap::SinkMode::APPEND
        else raise "Unrecognized sink mode '#{options[:sink_mode]}'"
      end

      local_scheme = scheme[:local_scheme]
      @local_tap = local_scheme ? Java::CascadingTapLocal::FileTap.new(local_scheme, path, sink_mode) : nil

      hadoop_scheme = scheme[:hadoop_scheme]
      @hadoop_tap = hadoop_scheme ? Java::CascadingTapHadoop::Hfs.new(hadoop_scheme, path, sink_mode) : nil
    end
  end

  # A Cascading::MultiTap represents one of Cascading's aggregate taps and is
  # built via static constructors that accept an array of Cascading::Taps.  In
  # order for a mode (Cascading local or Hadoop) to be supported, all provided
  # taps must support it.
  class MultiTap < BaseTap
    def initialize(local_tap, hadoop_tap)
      super(local_tap, hadoop_tap)
    end

    def self.multi_source_tap(taps)
      multi_tap(taps, Java::CascadingTap::MultiSourceTap)
    end

    def self.multi_sink_tap(taps)
      multi_tap(taps, Java::CascadingTap::MultiSinkTap)
    end

    private

    def self.multi_tap(taps, klass)
      local_supported = taps.all?{ |tap| tap.local? }
      local_tap = local_supported ? klass.new(taps.map{ |tap| tap.local_tap }.to_java('cascading.tap.Tap')) : nil

      hadoop_supported = taps.all?{ |tap| tap.hadoop? }
      hadoop_tap = hadoop_supported ? klass.new(taps.map{ |tap| tap.hadoop_tap }.to_java('cascading.tap.Tap')) : nil

      MultiTap.new(local_tap, hadoop_tap)
    end
  end
end
