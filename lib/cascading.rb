require 'java'

module Cascading
  # :stopdoc:
  VERSION = '1.0.0'
end

require 'cascading/aggregations'
require 'cascading/assembly'
require 'cascading/base'
require 'cascading/cascade'
require 'cascading/cascading'
require 'cascading/cascading_exception'
require 'cascading/expr_stub'
require 'cascading/filter_operations'
require 'cascading/flow'
require 'cascading/identity_operations'
require 'cascading/mode'
require 'cascading/operations'
require 'cascading/regex_operations'
require 'cascading/scope'
require 'cascading/sub_assembly'
require 'cascading/tap'
require 'cascading/text_operations'

# include module to make it available at top level
include Cascading
