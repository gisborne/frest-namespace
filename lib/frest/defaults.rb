require_relative('tap_h')

module Frest
  module Defaults
    require 'pp'

    extend TapH

    DEFAULT_SUBTABLES = %w{simple arguments local_fns remote_fns}
    DEFAULT_DB        = 'default.sqlite'
    DEFAULT_STORE_ID  = 'default'
    DEFAULT_BRANCH_ID = 'root'
    LOG_SQL           = true

    @@connections = {}

    tap_h def execute(
        db: DEFAULT_DB,
        connection: get_connection(file: db),
        sql: '',
        log: LOG_SQL,
        c_:,
        **_
    )
      pp "#{sql}\n\n" if log
      connection.execute sql
    end

    tap_h def get_connection(
        file: DEFAULT_DB,
        c_:,
        **_
    )
      f = File.absolute_path(file) #canonicalize connection by full path
      return @@connections[f] if @@connections[f]
      @@connections[f] = SQLite3::Database.new(f)
      result = @@connections[f]
      result.create_function 'uuid', 0 do |func, value|
        func.result = SecureRandom.uuid
      end

      result
    end
  end
end