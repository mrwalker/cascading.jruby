require 'cascading'

module MockAssemblies
  def mock_assembly(&block)
    assembly = nil
    flow 'test' do
      source 'test', tap('test/data/data1.txt')
      assembly = assembly 'test', &block
      sink 'test', tap('output/test_mock_assembly')
    end
    assembly
  end

  def mock_branched_assembly(&block)
    assembly = nil
    flow 'mock_branched_assembly' do
      source 'data1', tap('test/data/data1.txt')

      assembly 'data1' do
        branch 'test1' do
          pass
        end
        branch 'test2' do
          pass
        end
      end

      assembly = assembly 'test', &block

      sink 'test', tap('output/test_mock_branched_assembly')
    end
    assembly
  end

  def mock_two_input_assembly(&block)
    assembly = nil
    flow 'mock_two_input_assembly' do
      source 'test1', tap('test/data/data1.txt')
      source 'test2', tap('test/data/data2.txt')

      assembly 'test1' do
        split 'line', /[.,]*\s+/, ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      end

      assembly 'test2' do
        split 'line', /[.,]*\s+/, ['name',  'id', 'town'], :output => ['name',  'id', 'town']
      end

      assembly = assembly 'test', &block

      sink 'test', tap('output/test_mock_two_input_assembly')
    end
    assembly
  end
end
