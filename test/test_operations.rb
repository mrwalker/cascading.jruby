require 'test/unit'
require 'cascading'

class TC_Operations < Test::Unit::TestCase
  include Operations

  def test_coerce_to_java_int
    result = coerce_to_java(1)

    assert_equal Java::JavaLang::Long, result.class
    assert_equal 1.to_java(java.lang.Long).java_class, result.java_class
    assert_equal 1.to_java(java.lang.Long), result
  end

  def test_coerce_to_java_long
    result = coerce_to_java(32503701600000)

    assert_equal Java::JavaLang::Long, result.class
    assert_equal 32503701600000.to_java(java.lang.Long).java_class, result.java_class
    assert_equal 32503701600000.to_java(java.lang.Long), result
  end

  def test_coerce_to_java_string
    result = coerce_to_java('a')

    assert_equal Java::JavaLang::String, result.class
    assert_equal 'a'.to_java(java.lang.String).java_class, result.java_class
    assert_equal 'a'.to_java(java.lang.String), result
  end

  def test_coerce_to_java_float
    result = coerce_to_java(5.4)

    assert_equal Java::JavaLang::Double, result.class
    assert_equal (5.4).to_java(java.lang.Double).java_class, result.java_class
    assert_equal (5.4).to_java(java.lang.Double), result
  end

  def test_coerce_to_java_double
    result = coerce_to_java(10e100)

    assert_equal Java::JavaLang::Double, result.class
    assert_equal (10e100).to_java(java.lang.Double).java_class, result.java_class
    assert_equal (10e100).to_java(java.lang.Double), result
  end

  def test_coerce_to_java_nil
    result = coerce_to_java(nil)

    assert_equal NilClass, result.class
    assert_nil result
  end

  def test_coerce_to_java_other
    orig_val = [1,2,3]
    result = coerce_to_java(orig_val)

    assert_equal Java::JavaLang::String, result.class
    assert_equal ''.to_java(java.lang.String).java_class, result.java_class
    assert_equal orig_val.to_s.to_java(java.lang.String), result
  end

  def test_to_java_comparable_array
    results = to_java_comparable_array([1, 'string', 1.5, nil])

    assert_equal results.map{|i| i.class}, [Fixnum, String, Float, NilClass]
  end
end
