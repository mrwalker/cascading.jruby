# -*- encoding: utf-8 -*-
#$: << File.join(File.dirname(__FILE__), '..', 'lib')
#require 'cascading'

Gem::Specification.new do |s|
  s.name = "cascading.jruby"
  # TODO: in 2.0.0, Job will encapsulate Cascading module, so we can directly
  # grab the version from there; for now, just hack it
  #s.version = Cascading::VERSION
  s.version = '1.0.0'
  s.date = Time.now.strftime('%Y-%m-%d')
  s.summary = "A JRuby DSL for Cascading"
  s.homepage = "http://github.com/mrwalker/cascading.jruby"
  s.email = "matt.r.walker@gmail.com"
  s.authors = ["Matt Walker", "Gr\303\251goire Marabout"]

  s.files = Dir.glob("lib/**/*.rb")
  s.test_files = Dir.glob("test/**/*.rb")
  s.require_paths = ["lib"]

  s.rdoc_options = ["--main", "README.md"]
  s.extra_rdoc_files = ["README.md", "LICENSE.txt"]

  s.description = "cascading.jruby is a small DSL above Cascading, written in JRuby"
end
