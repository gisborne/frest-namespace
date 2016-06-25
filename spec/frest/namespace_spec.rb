require 'spec_helper'

describe Frest::Namespace do
  n = Frest::Namespace

  n.setup

  describe "Basic CRUD" do
    it "can store a value and retrieve it" do
      n.set id: '69c9ed0a-b83a-4816-93e7-80a34265a949', values: {foo: 1, bar: 'bar'}
      expect(n.get id: '69c9ed0a-b83a-4816-93e7-80a34265a949').to match({'foo' => '1', 'bar' => 'bar'})
    end

    it "can delete an entire hash and it's gone" do
      n.set id: '3ff7bacf-1511-4709-b84f-2427e91b01e8', values: {a: 1}
      n.delete id: '3ff7bacf-1511-4709-b84f-2427e91b01e8'
      expect(n.get(id: '3ff7bacf-1511-4709-b84f-2427e91b01e8').count).to match(0)
    end
  end
end
