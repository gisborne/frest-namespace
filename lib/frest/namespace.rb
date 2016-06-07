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
        **context)

      c = get_connection file: db

      insert_hash = values.clone
      insert_hash.each { |k, v|
        insert_hash[k] = prepare_value(value: v, **context)
      }

      keys_string   = "(id, #{insert_hash.keys * ','})"
      values_string = insert_hash.map do |k, v|
        "('#{id}', '#{k}', #{v})"
      end * ",\n"

      #TODO respect subtables
      sql           = %{
              INSERT OR REPLACE INTO #{store_id}_simple(id, key, value)
              VALUES#{values_string}
      }
      p sql
      c.execute sql
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
