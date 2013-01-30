namespace :ant do
  desc 'Retrieves Cascading and Hadoop jars and sets environment variables to point to them'
  task :retrieve do
    if File.exists?('build/ivy/resolved') && File.ctime('build/ivy/resolved') > File.ctime('ivy.xml')
      puts 'Ivy dependencies already resolved'
    else
      raise 'Ant retrieve failed' unless system('ant retrieve')
      `touch build/ivy/resolved`
    end
    Dir.glob('build/lib/*.jar').each do |jar|
      $CLASSPATH << jar
    end
  end

  desc 'Builds Java source for inclusion in gem'
  task :build do
    raise 'Ant build failed' unless system('ant build')
  end

  desc 'Cleans Java build files'
  task :clean do
    system('ant clean')
  end
end
