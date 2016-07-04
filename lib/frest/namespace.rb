# require "frest/namespace/version"
require 'sqlite3'
require 'securerandom'
require_relative 'tap_h'
require_relative 'setup'
require_relative 'defaults'

module Frest
  module Namespace
    include TapH
    include Defaults

    extend self

    tap_h def set(
        value:,
        store_id: DEFAULT_STORE_ID,
        db: DEFAULT_DB,
        insertfn: method(:insert_value),
        deletefn: method(:delete),
        c_:,
        **_)
            
      insert_hash, delete_hash = value.partition { |_, v| v }.map(&:to_h)
      
      insertfn.call(
          store_id: store_id,
          value: insert_hash,
          **c_)
      
      deletefn.call(
          store_id: store_id,
          keys: delete_hash.keys,
          **c_)
    end

    tap_h def delete(
        id:,
        db: DEFAULT_DB,
        store_id: DEFAULT_STORE_ID,
        keys: nil,
        c_:,
        **_)
            
      return if keys != nil && keys.count == 0

      #TODO respect subtables

      sql = %{
        DELETE FROM #{store_id}_simple
        WHERE
          id = '#{id}'
      }
      sql += %{
        AND
          key IN (#{keys.map { |k| "'#{k}'" } * ','})
      } if keys

      execute(
          sql: sql,
          **c_)
    end


    tap_h def get(
        id:,
        store_id: DEFAULT_STORE_ID,
        db: DEFAULT_DB,
        subtables: DEFAULT_SUBTABLES,
        c_:,
        **_)
            
      #TODO respect subtables
      result = execute(
          sql: %{
            SELECT
              key,
              value
            FROM
              #{store_id}_simple
            WHERE
              id = '#{id}'},
           **c_)
      result.to_h
    end


    tap_h def prepare_value(
        value:,
        c_:,
        **_)
      if (value.is_a? Hash)
        result = uuid

        set(
          **c_.merge(id: result))

        "'#{result}'"
      else
        "'#{SQLite3::Database.quote(value.to_s)}'"
      end
    end

    tap_h def execute(
      db: DEFAULT_DB,
      connection: get_connection(file: db),
      sql: '',
      log: LOG_SQL,
      c_:,
      **_
    )
      p "#{sql}\n\n" if log
      connection.execute sql
    end


    private

    tap_h def insert_value(
        id:,
        value:,
        store_id:,
        connection: get_connection,
        c_:,
        **_)
      return if value.count == 0
      value.each { |k, v|
        value[k] = prepare_value(**c_.merge(value: v))
      }

      keys_string   = "(id, #{value.keys * ','})"
      value_string = value.map do |k, v|
        "('#{id}', '#{k}', #{v})"
      end * ",\n"

      #TODO respect subtables
      sql = %{
         INSERT OR REPLACE INTO #{store_id}_simple(id, key, value)
         values#{value_string}
      }

      execute sql: sql
    end

    def uuid
      SecureRandom.uuid
    end
  end
end