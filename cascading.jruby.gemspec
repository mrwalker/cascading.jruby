# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "cascading.jruby"
  s.version = "0.0.10"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Matt Walker", "Gr\303\251goire Marabout"]
  s.description = "cascading.jruby is a small DSL above Cascading, written in JRuby"
  s.email = "mwalker@etsy.com"
  s.extra_rdoc_files = ["LICENSE.txt"]
  s.files = Dir.glob("lib/**/*.rb")
  s.homepage = "http://github.com/etsy/cascading.jruby"
  s.rdoc_options = ["--main", "README.md"]
  s.require_paths = ["lib"]
  s.rubyforge_project = "cascading.jruby"
  s.rubygems_version = "1.8.21"
  s.summary = "A JRuby DSL for Cascading"
  s.test_files = Dir.glob("test/**/*.rb")

  if s.respond_to? :specification_version then
    s.specification_version = 3
  end
end
