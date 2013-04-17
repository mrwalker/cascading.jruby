module Cascading
  module AggregatorOperations
    def aggregator_function(args, aggregator_klass)
      options = args.extract_options!
      ignore = options[:ignore]

      parameters = [Cascading.fields(args), ignore].compact
      aggregator_klass.new(*parameters)
    end

    def first_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::First)
    end

    def min_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Min)
    end

    def max_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Max)
    end

    def last_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Last)
    end
  end
end
