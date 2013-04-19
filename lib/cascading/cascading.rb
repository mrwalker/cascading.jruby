require 'cascading/expr_stub'

module Cascading
  # Mapping that defines a convenient syntax for specifying Java classes, used
  # in Janino expressions and elsewhere.
  JAVA_TYPE_MAP = {
    :int => java.lang.Integer.java_class, :long => java.lang.Long.java_class,
    :bool => java.lang.Boolean.java_class, :double => java.lang.Double.java_class,
    :float => java.lang.Float.java_class, :string => java.lang.String.java_class,
  }

  # FIXME: I consider $jobconf_properties to be a hack forced on us by the lack
  # of properties handling in earlier versions of the gem.  Fully removing the
  # hack would look like introducing a Job abstraction which instantiates user
  # code, and allowing jading's runner to pass properties into that.  I've
  # already taken the step to thread properties through cascades and flows
  # rather than merge properties before connect, but we still require the
  # global properties hack to integrate with external runner code (jading).
  #
  # Note that this would also mean we can get rid of the global "registries" of
  # cascades and flows.  I've already eliminated most uses of these registries,
  # but they are still required for the runner to find user code required in a
  # previous step.  A Job abstraction would clean this up, as well.
  #
  # For now, it is important that people use these constructors rather than
  # directly building their own cascades and flows so that jading can send them
  # default properties.

  # Builds a top-level cascade given a name and a block.  Optionally accepts a
  # :mode, as explained in Cascading::Cascade#initialize.
  def cascade(name, options = {}, &block)
    raise "Could not build cascade '#{name}'; block required" unless block_given?
    raise 'Cascading::cascade does not accept the :properties param only the global $jobconf_properties' if options[:properties]

    options[:properties] = $jobconf_properties.dup if defined?($jobconf_properties) && $jobconf_properties

    cascade = Cascade.new(name, options)
    cascade.instance_eval(&block)
    cascade
  end

  # Builds a top-level flow given a name and block for applications built of
  # flows with no cascades.  Optionally accepts a :mode, as explained in
  # Cascading::Flow#initialize.
  def flow(name, options = {}, &block)
    raise "Could not build flow '#{name}'; block required" unless block_given?
    raise 'Cascading::flow does not accept the :properties param only the global $jobconf_properties' if options[:properties]

    options[:properties] = $jobconf_properties.dup if defined?($jobconf_properties) && $jobconf_properties

    flow = Flow.new(name, nil, options)
    flow.instance_eval(&block)
    flow
  end

  def describe
    Cascade.all.map{ |cascade| cascade.describe }.join("\n")
  end
  alias desc describe

  # See ExprStub.expr
  def expr(expression, options = {})
    ExprStub.expr(expression, options)
  end

  # Creates a cascading.tuple.Fields instance from a string or an array of strings.
  def fields(fields)
    if fields.nil?
      return nil
    elsif fields.is_a? Java::CascadingTuple::Fields
      return fields
    elsif fields.is_a? ::Array
      if fields.size == 1
        return fields(fields[0])
      end
      raise "Fields cannot be nil: #{fields.inspect}" if fields.include?(nil)
    end
    return Java::CascadingTuple::Fields.new([fields].flatten.map{ |f| f.kind_of?(Fixnum) ? java.lang.Integer.new(f) : f }.to_java(java.lang.Comparable))
  end

  def all_fields
    Java::CascadingTuple::Fields::ALL
  end

  def last_grouping_fields
    Java::CascadingTuple::Fields::VALUES
  end

  # Computes fields formed by removing remove_fields from base_fields.  Operates
  # only on named fields, not positional fields.
  def difference_fields(base_fields, remove_fields)
    fields(base_fields.to_a - remove_fields.to_a)
  end

  # Combines fields deduplicating them with trailing underscores as necessary.
  # This is used in joins to avoid requiring the caller to unique fields before
  # they are joined.
  def dedup_fields(*fields)
    raise 'Can only be applied to declarators' unless fields.all?{ |f| f.is_declarator? }
    fields(dedup_field_names(*fields.map{ |f| f.to_a }))
  end

  # Helper used by dedup_fields that operates on arrays of field names rather
  # than fields objects.
  def dedup_field_names(*names)
    names.inject([]) do |acc, arr|
      acc + arr.map{ |e| search_field_name(acc, e) }
    end
  end

  def search_field_name(names, candidate)
    names.include?(candidate) ? search_field_name(names, "#{candidate}_") : candidate
  end
  private :search_field_name

  # Creates a TextLine scheme (can be used in both Cascading local and hadoop
  # modes).  Positional args are used if <tt>:source_fields</tt> is not
  # provided.
  #
  # The named options are:
  # [source_fields] Fields to be read from a source with this scheme.  Defaults
  #                 to ['offset', 'line'].
  # [sink_fields] Fields to be written to a sink with this scheme.  Defaults to
  #               all_fields.
  # [compression] A symbol, either <tt>:enable</tt> or <tt>:disable</tt>, that
  #               governs the TextLine scheme's compression.  Defaults to the
  #               default TextLine compression (only applies to c.s.h.TextLine).
  def text_line_scheme(*args_with_options)
    options, source_fields = args_with_options.extract_options!, args_with_options
    source_fields = fields(options[:source_fields] || (source_fields.empty? ? ['offset', 'line'] : source_fields))
    sink_fields = fields(options[:sink_fields]) || all_fields
    sink_compression = case options[:compression]
      when :enable  then Java::CascadingSchemeHadoop::TextLine::Compress::ENABLE
      when :disable then Java::CascadingSchemeHadoop::TextLine::Compress::DISABLE
      else Java::CascadingSchemeHadoop::TextLine::Compress::DEFAULT
    end

    {
      :local_scheme => Java::CascadingSchemeLocal::TextLine.new(source_fields, sink_fields),
      :hadoop_scheme => Java::CascadingSchemeHadoop::TextLine.new(source_fields, sink_fields, sink_compression),
    }
  end

  # Creates a c.s.h.SequenceFile scheme instance from the specified fields.  A
  # local SequenceFile scheme is not provided by Cascading, so this scheme
  # cannot be used in Cascading local mode.
  def sequence_file_scheme(*fields)
    {
      :local_scheme => nil,
      :hadoop_scheme => Java::CascadingSchemeHadoop::SequenceFile.new(fields.empty? ? all_fields : fields(fields)),
    }
  end

  def multi_source_tap(*taps)
    MultiTap.multi_source_tap(taps)
  end

  def multi_sink_tap(*taps)
    MultiTap.multi_sink_tap(taps)
  end

  # Creates a Cascading::Tap given a path and optional :scheme and :sink_mode.
  def tap(path, options = {})
    Tap.new(path, options)
  end

  # Constructs properties to be passed to Flow#complete or Cascade#complete
  # which will locate temporary Hadoop files in base_dir.  It is necessary to
  # pass these properties only when executing scripts in Hadoop local mode via
  # JRuby's main method, which confuses Cascading's attempt to find the
  # containing jar.  When using Cascading local mode, these are unnecessary.
  def local_properties(base_dir)
    dirs = {
      'test.build.data' => "#{base_dir}/build",
      'hadoop.tmp.dir' => "#{base_dir}/tmp",
      'hadoop.log.dir' => "#{base_dir}/log",
    }
    dirs.each{ |key, dir| `mkdir -p #{dir}` }

    job_conf = Java::OrgApacheHadoopMapred::JobConf.new
    job_conf.jar = dirs['test.build.data']
    dirs.each{ |key, dir| job_conf.set(key, dir) }

    job_conf.num_map_tasks = 1
    job_conf.num_reduce_tasks = 1

    properties = java.util.HashMap.new
    Java::CascadingFlowHadoopPlanner::HadoopPlanner.copy_job_conf(properties, job_conf)
    properties
  end
end
