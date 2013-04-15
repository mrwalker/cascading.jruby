require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

context Cascading do
  it 'should handle string and integer field names' do
    f = fields(['a', 1, 'b', 2])
    f.to_a.should == ['a', 1, 'b', 2]
  end

  it 'should dedup field names from multiple sources' do
    left_names = ['a', 'b', 'c', 'd', 'e']
    mid_names = ['a', 'f']
    right_names = ['a', 'g']

    field_names = dedup_field_names(left_names, mid_names, right_names)
    field_names.should == [
      'a', 'b', 'c', 'd', 'e',
      'a_', 'f',
      'a__', 'g'
    ]
  end

  it 'should fail to resolve duplicate fields' do
    incoming = fields(['line'])
    declared = fields(['line'])
    outgoing = all_fields
    lambda do
      begin
        resolved = Java::CascadingTuple::Fields.resolve(outgoing, [incoming, declared].to_java(Java::CascadingTuple::Fields))
      rescue NativeException => e
        raise e.cause
      end
    end.should raise_error Java::CascadingTuple::TupleException, 'field name already exists: line'
  end

  it 'should find branches to sink' do
    cascade 'branched_pass', :mode => :local do
      flow 'branched_pass' do
        source 'input', tap('spec/resource/test_input.txt', :scheme => text_line_scheme)
        assembly 'input' do
          branch 'branched_input' do
            project 'line'
          end
        end
        sink 'branched_input', tap("#{OUTPUT_DIR}/branched_pass_out", :sink_mode => :replace)
      end
    end.complete

    ilc = `wc -l spec/resource/test_input.txt`.strip.split(/\s+/).first
    olc = `wc -l #{OUTPUT_DIR}/branched_pass_out`.strip.split(/\s+/).first
    olc.should == ilc
  end

  it 'should create an isolated namespace per cascade' do
    cascade 'double', :mode => :local do
      flow 'double' do
        source 'input', tap('spec/resource/test_input.txt', :scheme => text_line_scheme)
        assembly 'input' do # Dup name
          insert 'doubled' => expr('line:string + "," + line:string')
          project 'doubled'
        end
        sink 'input', tap("#{OUTPUT_DIR}/double_out", :sink_mode => :replace)
      end
    end

    cascade 'pass', :mode => :local do
      flow 'pass' do
        source 'input', tap('spec/resource/test_input.txt', :scheme => text_line_scheme)
        assembly 'input' do # Dup name
          project 'line'
        end
        sink 'input', tap("#{OUTPUT_DIR}/pass_out", :sink_mode => :replace)
      end
    end

    Cascade.get('double').complete
    Cascade.get('pass').complete
    diff = `diff #{OUTPUT_DIR}/double_out #{OUTPUT_DIR}/pass_out`
    diff.should_not be_empty
  end

  it 'should support joins in branches' do
    cascade 'branch_join', :mode => :local do
      flow 'branch_join' do
        source 'left', tap('spec/resource/join_input.txt', :scheme => text_line_scheme)
        source 'right', tap('spec/resource/join_input.txt', :scheme => text_line_scheme)

        assembly 'left' do
          split 'line', /,/, ['x', 'y', 'z']
          project 'x', 'y', 'z'
        end

        assembly 'right' do
          split 'line', /,/, ['x', 'y', 'z']
          project 'x', 'y', 'z'

          branch 'branch_join' do
            join 'left', 'right', :on => 'x'
          end
        end

        sink 'branch_join', tap("#{OUTPUT_DIR}/branch_join_out.txt", :sink_mode => :replace)
      end
    end.complete
  end
end
