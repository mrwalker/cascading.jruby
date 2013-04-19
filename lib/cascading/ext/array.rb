# Extensions to Arrays in support of variable length lists of field names.  This
# is not pretty, but supports DSL features like:
#     group_by 'field1', 'field2', :sort_by => 'field3' do
#       ...
#     end
#
# The most obvious limitation of the approach is that function definitions of
# the form f(*args_with_options) are not self-documenting.  To compensate for
# this, documentation of all arguments and optional parameters must be provided
# on the DSL method.
class Array
  # Use this extension to extract the optional parameters from a
  # *args_with_options argument.
  # So if you have a function:
  #     def f(*args_with_options)
  # You can destructively process the args_with_options as follows:
  #     options, just_args = args_with_options.extract_options!, args_with_options
  def extract_options!
     last.is_a?(::Hash) ? pop : {}
  end
  
  # Non-destructive form of Array#extract_options!
  def extract_options
     last.is_a?(::Hash) ? last : {}
  end
end
