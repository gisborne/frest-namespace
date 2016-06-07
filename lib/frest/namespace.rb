# require "frest/namespace/version"
require 'sqlite3'
require 'securerandom'

module Frest
  module Namespace
    extend self

    @connections = {}

    DEFAULT_SUBTABLES = %w{simple arguments local_fns remote_fns}
    DEFAULT_DB        = 'default.sqlite'
    DEFAULT_STORE_ID  = 'default'

    def get_connection(
        file:
    )
      f               = File.absolute_path(file) #canonicalize connection by full path
      @connections[f] ||= SQLite3::Database.new(f)
    end

    def set(
        store_id: DEFAULT_STORE_ID,
        id:,
        db: DEFAULT_DB,
        values:,
        insert_values: method(:insert_values),
        delete_values: method(:delete_values),
        **context)

      c = get_connection file: db

      insert_hash, delete_hash = values.partition{|k, v| v}.map &:to_h
      insert_values(connection: c, id: id, insert_hash: insert_hash, store_id: store_id, **context)
      delete_values(connection: c, id: id, keys: delete_hash.keys, store_id: store_id, **context)
    end

    def insert_values connection:, id:, insert_hash:, store_id:, **context
      return if insert_hash.count == 0
      insert_hash.each { |k, v|
        insert_hash[k] = prepare_value(value: v, **context)
      }

      keys_string   = "(id, #{insert_hash.keys * ','})"
      values_string = insert_hash.map do |k, v|
        "('#{id}', '#{k}', #{v})"
      end * ",\n"

      #TODO respect subtables
      sql = %{
        INSERT OR REPLACE INTO #{store_id}_simple(id, key, value)
        VALUES#{values_string}
      }

      connection.execute sql
    end


    def delete_values connection:, id:, keys:, store_id:, **context
      return if keys.count == 0
      #TODO respect subtables
      sql = %{
        DELETE FROM #{store_id}_simple
        WHERE
          id = '#{id}' AND
          key IN (#{keys.map{|k| "'#{k}'"} * ','})
      }

      connection.execute sql
    end


    def get(
        store_id: DEFAULT_STORE_ID,
        id:,
        db: DEFAULT_DB,
        subtables: DEFAULT_SUBTABLES,
        **context)

      c      = get_connection file: db

      #TODO respect subtables
      result = c.execute %{
          SELECT
            key,
            value
          FROM
            #{store_id}_simple
          WHERE
            id = '#{id}'
                }
      result.to_h
    end



    def setup(
        id: 'default',
        db: DEFAULT_DB,
        subtables: DEFAULT_SUBTABLES,
        **context)

      c = get_connection file: db

      subtables.each do |subtbl|
        c.execute %{
            CREATE TABLE #{id}_#{subtbl}(
              id UUID NOT NULL,
              key text NOT NULL,
              value text,
              PRIMARY KEY(id, key)
            )
          }
      end
    end


    def prepare_value value:, **context
      "'#{SQLite3::Database.quote(value.to_s)}'"
    end
  end
end
