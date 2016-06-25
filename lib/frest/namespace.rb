# require "frest/namespace/version"
require 'sqlite3'
require 'securerandom'
require_relative 'tap_h'

module Frest
  module Namespace
    include TapH
    extend self

    @connections = {}

    DEFAULT_SUBTABLES = %w{simple arguments local_fns remote_fns}
    DEFAULT_DB        = 'default.sqlite'
    DEFAULT_STORE_ID  = 'default'
    LOG_SQL           = true

    tap_h def get_connection(
        file: DEFAULT_DB,
        c_:,
        **_
    )
      f = File.absolute_path(file) #canonicalize connection by full path
      @connections[f] ||= SQLite3::Database.new(f)
      result = @connections[f]
      result.create_function 'uuid', 0 do |func, value|
        func.result = SecureRandom.uuid
      end

      result
    end

    tap_h def set(
        values:,
        store_id: DEFAULT_STORE_ID,
        db: DEFAULT_DB,
        insertfn: method(:insert_values),
        deletefn: method(:delete),
        c_:,
        **_)
            
      insert_hash, delete_hash = values.partition { |_, v| v }.map(&:to_h)
      
      insertfn.call(
          store_id: store_id,
          insert_hash: insert_hash,
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


    tap_h def setup(
        id: 'default',
        subtables: DEFAULT_SUBTABLES,
        c_:,
        **_)

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

        execute(
            sql: sql,
            **c_)

        execute(
            sql: %{
              CREATE TABLE IF NOT EXISTS #{id}_#{subtbl}_history_src(
                id UUID NOT NULL,
                key text NOT NULL,
                value text,
                created DATE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(id, key, created)
              )
            },
            **c_)

        execute(
            sql:%{
              CREATE TABLE IF NOT EXISTS #{id}_#{subtbl}_deleted(
                id UUID NOT NULL,
                deleted DATE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(id)
              )
            },
            **c_)

        execute(
            sql: "
              CREATE VIEW IF NOT EXISTS
                #{id}_#{subtbl} AS
              SELECT
                *
              FROM
                #{id}_#{subtbl}_src
                LEFT OUTER JOIN #{id}_#{subtbl}_deleted ON (#{id}_#{subtbl}_deleted.id = #{id}_#{subtbl}_src.id)
              WHERE
                #{id}_#{subtbl}_src.id IS NULL",
            **c_)

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

        execute(
            sql: sql,
            **c_)

        sql = %{
          CREATE TRIGGER IF NOT EXISTS
            #{id}_#{subtbl}_delete_trigger
          INSTEAD OF
            DELETE
          ON
            #{id}_#{subtbl}
          FOR EACH ROW
          BEGIN
            INSERT INTO
              #{id}_#{subtbl}_deleted(
                id)
              SELECT
                OLD.id;
          END
        }

        execute(
            sql: sql,
            **c_)
      end
    end


    tap_h def prepare_value(
        value:,
        c_:,
        **_)
      "'#{SQLite3::Database.quote(value.to_s)}'"
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

    tap_h def insert_values(
        id:,
        insert_hash:,
        store_id:,
        connection: get_connection,
        c_:,
        **_)
      return if insert_hash.count == 0
      insert_hash.each { |k, v|
        insert_hash[k] = prepare_value(value: v, **c_)
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
  end
end