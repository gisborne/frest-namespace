File.delete('default.sqlite') rescue nil
$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'frest/namespace'
