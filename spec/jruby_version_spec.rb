require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

context Object do
  it 'should definitely not do this' do
    names = ['x', 'y'].to_java(java.lang.String)
    types = [java.lang.Integer.java_class, java.lang.Integer.java_class].to_java(java.lang.Class)
    evaluator = Java::OrgCodehausJanino::ExpressionEvaluator.new('x + y', java.lang.Comparable.java_class, names, types)

    thrown = nil
    exception = nil
    begin
      evaluator.evaluate([nil, nil].to_java)
    rescue java.lang.IllegalArgumentException => iae
      thrown = 'IllegalArgumentException'
      exception = iae
    rescue java.lang.reflect.InvocationTargetException => ite
      thrown = 'InvocationTargetException'
      exception = ite
    end

    thrown.should == 'InvocationTargetException'
    if JRUBY_VERSION == '1.7.0' || JRUBY_VERSION == '1.7.3'
      exception.java_class.should be Java::JavaLangReflect::InvocationTargetException.java_class
    else
      # How can this be?  A nil exception?
      exception.should be_nil
    end
  end

  case JRUBY_VERSION
  when '1.2.0'
    it 'should handle Fixnum -> Long for ExprStub#eval' do
      e = ExprStub.new('x:long + y:long')
      result = e.eval(:x => 2, :y => 3)
      result.should == 5
    end

    it 'should handle Fixnum -> Long for ExprStub#validate' do
      e = ExprStub.new('x:long + y:long')
      result = e.validate
      result.should == 0
    end
  when '1.4.0'
    # This test previously failed for 1.4.0 (it's duplicated in cascading_spec)
    it 'should handle string and integer field names' do
      f = fields(['a', 1, 'b', 2])
      f.to_a.should == ['a', 1, 'b', 2]
    end

    it 'should handle Fixnum -> Integer for ExprStub#eval' do
      e = ExprStub.new('x:int + y:int')
      result = e.eval(:x => 2, :y => 3)
      result.should == 5
    end

    it 'should handle Fixnum -> Integer for ExprStub#validate' do
      e = ExprStub.new('x:int + y:int')
      result = e.validate
      result.should == 0
    end
  when '1.5.3', '1.6.5', '1.6.7.2', '1.7.0', '1.7.3'
    it 'should handle Fixnum -> Integer for ExprStub#eval' do
      e = ExprStub.new('x:int + y:int')
      result = e.eval(:x => 2, :y => 3)
      result.should == 5
    end

    it 'should handle Fixnum -> Integer for ExprStub#validate' do
      e = ExprStub.new('x:int + y:int')
      result = e.validate
      result.should == 0
    end
  else
    raise "cascading.jruby has not been tested with JRuby version #{JRUBY_VERSION}"
  end
end
