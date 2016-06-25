module TapH
  def tap_h(fn)
    untapped_method = instance_method(fn)
    untapped_name   = (fn.to_s + '_untapped').to_sym
    define_method untapped_name, untapped_method

    define_method fn do |**c, &block|
      untapped_method.bind(self).call(c_: c, **c, &block)
    end
  end
end