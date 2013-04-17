require 'java'

module Cascading
  # :stopdoc:
  VERSION = '0.0.10'
end

require 'cascading/assembly'
require 'cascading/base'
require 'cascading/cascade'
require 'cascading/cascading'
require 'cascading/cascading_exception'
require 'cascading/expr_stub'
require 'cascading/flow'
require 'cascading/mode'
require 'cascading/operations'
require 'cascading/identity_operations'
require 'cascading/regex_operations'
require 'cascading/text_operations'
require 'cascading/scope'
require 'cascading/tap'

# include module to make them available at top package
include Cascading
