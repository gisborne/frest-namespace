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
        file: DEFAULT_DB
    )
      f               = File.absolute_path(file) #canonicalize connection by full path
      @connections[f] ||= SQLite3::Database.new(f)
      result = @connections[f]
      result.create_function 'uuid', 0 do |func, value|
        func.result = SecureRandom.uuid
      end

      result
    end

    def set(
        id:,
        values:,
        store_id: DEFAULT_STORE_ID,
        db: DEFAULT_DB,
        insertfn: method(:insert_values),
        deletefn: method(:delete),
        **c)

      c = get_connection file: db

      insert_hash, delete_hash = values.partition { |_, v| v }.map &:to_h
      insertfn.call(connection: get_connection, id: id, insert_hash: insert_hash, store_id: store_id, **c)
      deletefn.call(connection: get_connection, id: id, keys: delete_hash.keys, store_id: store_id, **c)
    end

    def delete(
        connection:,
        id:,
        store_id:,
        keys: nil,
        **c)
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

      connection.execute sql
    end


    def get(
        store_id: DEFAULT_STORE_ID,
        id:,
        db: DEFAULT_DB,
        subtables: DEFAULT_SUBTABLES,
        **c)

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
        **c)

      c = get_connection file: db

      subtables.each do |subtbl|
        sql = %{
          CREATE TABLE IF NOT EXISTS #{id}_#{subtbl}_src(
            id UUID NOT NULL,
            key text NOT NULL,
            value text,
            created date DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(id, key)
          )
        }

        c.execute sql

        c.execute %{
          CREATE TABLE IF NOT EXISTS #{id}_#{subtbl}_history_src(
            id UUID NOT NULL,
            key text NOT NULL,
            value text,
            created date DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(id, key, created)
          )
        }

        c.execute "
          CREATE VIEW IF NOT EXISTS
            #{id}_#{subtbl} AS
          SELECT
            *
          FROM
            #{id}_#{subtbl}_src"

        sql = %{
          CREATE TRIGGER IF NOT EXISTS
            #{id}_#{subtbl}_uuid_trigger
          INSTEAD OF
            INSERT
          ON
            #{id}_#{subtbl}
          FOR EACH ROW
          BEGIN
            INSERT INTO
              #{id}_#{subtbl}_src(
                id,
                key,
                value)
              SELECT
                COALESCE(NEW.id, UUID()),
                NEW.key,
                NEW.value;
          END
        }

        # p sql
        c.execute sql
      end
    end


    def prepare_value(
        value:,
        **c)
      "'#{SQLite3::Database.quote(value.to_s)}'"
    end


    private

    def insert_values(
        connection: get_connection,
        id:,
        insert_hash:,
        store_id:,
        **c)
      return if insert_hash.count == 0
      insert_hash.each { |k, v|
        insert_hash[k] = prepare_value(value: v, **c)
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

      connection.execute sql
    end
  end
end