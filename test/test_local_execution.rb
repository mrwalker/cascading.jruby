require 'test/unit'
require 'cascading'

class TC_LocalExecution < Test::Unit::TestCase
  def test_smoke_test_multi_source_tap
    cascade 'splitter', :mode => :local do
      flow 'splitter' do
        tap1 = tap 'test/data/data1.txt'
        tap2 = tap 'test/data/data2.txt'
        source 'data', multi_source_tap(tap1, tap2)

        assembly 'data' do
          pass
        end

        sink 'data', tap('output/test_smoke_test_multi_source_tap')
      end
    end.complete
  end

  def test_smoke_test_sequence_file_scheme
    cascade 'smoke', :mode => :local do
      flow 'smoke' do
        source 'input', tap('test/data/data1.txt')
        assembly 'input' do
          pass
        end
        compress_output :default, :block
        sink 'input', tap('output/test_smoke_test_sequence_file_scheme', :scheme => sequence_file_scheme)
      end
    end.complete
  end

  def test_splitter
    flow = flow 'splitter', :mode => :local do
      source 'copy', tap('test/data/data1.txt')

      assembly 'copy' do
        split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
        assert_size_equals 4
        assert_not_null
        debug :print_fields => true
      end

      sink 'copy', tap('output/test_splitter', :sink_mode => :replace)
    end.complete
  end

  def test_smoke_test_multi_source_tap
    cascade 'multi_source_tap', :mode => :local do
      flow 'multi_source_tap' do
        tap1 = tap 'test/data/data1.txt'
        tap2 = tap 'test/data/data2.txt'
        source 'data', multi_source_tap(tap1, tap2)

        assembly 'data' do
          pass
        end

        sink 'data', tap('output/test_smoke_test_multi_source_tap')
      end
    end.complete
  end

  def test_join1
    join_grouping_fields, join_values_fields = nil, nil
    cascade 'splitter', :mode => :local do
      flow 'splitter' do
        source 'data1', tap('test/data/data1.txt')
        source 'data2', tap('test/data/data2.txt')

        assembly1 = assembly 'data1' do
          split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
          assert_size_equals 4
          assert_not_null
          debug :print_fields => true
        end

        assembly2 = assembly 'data2' do
          split 'line', /[.,]*\s+/, ['name', 'id', 'town'], :output => ['name', 'id', 'town']
          assert_size_equals 3
          assert_not_null
          debug :print_fields => true
        end

        assembly 'joined' do
          join assembly1.name, assembly2.name, :on => ['name', 'id'], :declared_fields => ['name', 'score1', 'score2', 'id', 'name2', 'id2', 'town']
          join_grouping_fields = scope.grouping_fields.to_a
          join_values_fields = scope.values_fields.to_a

          assert_size_equals 7
          assert_not_null
        end

        sink 'joined', tap('output/test_join1', :sink_mode => :replace)
      end
    end.complete
    assert_equal ['name', 'id', 'name_', 'id_'], join_grouping_fields
    assert_equal ['name', 'score1', 'score2', 'id', 'name2', 'id2', 'town'], join_values_fields
  end

  def test_join2
    join_grouping_fields, join_values_fields = nil, nil
    flow = flow 'splitter', :mode => :local do
      source 'data1', tap('test/data/data1.txt')
      source 'data2', tap('test/data/data2.txt')

      assembly 'data1' do
        split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
        debug :print_fields => true
      end

      assembly 'data2' do
        split 'line', /[.,]*\s+/, ['name', 'code', 'town'], :output => ['name', 'code', 'town']
        debug :print_fields => true
      end

      assembly 'joined' do
        join :on => {'data1' => ['name', 'id'], 'data2' => ['name', 'code']}, :declared_fields => ['name', 'score1', 'score2', 'id', 'name2', 'code', 'town']
        join_grouping_fields = scope.grouping_fields.to_a
        join_values_fields = scope.values_fields.to_a
      end

      sink 'joined', tap('output/test_join2', :sink_mode => :replace)
     end.complete
     assert_equal ['name', 'id', 'name_', 'code'], join_grouping_fields
     assert_equal ['name', 'score1', 'score2', 'id', 'name2', 'code', 'town'], join_values_fields
   end

  def test_union
    union_grouping_fields, union_values_fields = nil, nil
    cascade 'union', :mode => :local do
      flow 'union' do
        source 'data1', tap('test/data/data1.txt')
        source 'data2', tap('test/data/data2.txt')

        assembly 'data1' do
          split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
          assert_size_equals 4
          assert_not_null

          project 'name', 'id'
          assert_size_equals 2
        end

        assembly 'data2' do
          split 'line', /[.,]*\s+/, ['name', 'code', 'town'], :output => ['name', 'code', 'town']
          assert_size_equals 3
          assert_not_null

          rename 'code' => 'id'
          project 'name', 'id'
          assert_size_equals 2
        end

        assembly 'union' do
          union 'data1', 'data2'
          union_grouping_fields = scope.grouping_fields.to_a
          union_values_fields = scope.values_fields.to_a
        end

        sink 'union', tap('output/test_union', :sink_mode => :replace)
     end
    end.complete
    assert_equal ['name'], union_grouping_fields
    assert_equal ['name', 'id'], union_values_fields
  end
end
