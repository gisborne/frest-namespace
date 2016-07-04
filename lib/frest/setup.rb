require_relative 'tap_h'
require_relative 'defaults'

module Frest
  module Setup
    include TapH
    include Defaults

    extend self

    tap_h def setup(
        id: 'default',
        subtables: DEFAULT_SUBTABLES,
        c_:,
        **_)

      subtables.each do |subtbl|
        name = "#{id}_#{subtbl}"

        #Basic key-values
        execute(
          sql: %{
            CREATE TABLE IF NOT EXISTS #{name}_src(
              id UUID NOT NULL,
              branch_id UUID NOT NULL,
              key text NOT NULL,
              value text,
              created date DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY(id, key)
            )
          },
          **c_
        )

        execute(
          sql: %{
            CREATE UNIQUE INDEX IF NOT EXISTS
              #{name}_branch_date_idx
            ON
              #{name}_src(
                id,
                branch_id,
                created,
                key
              )
          }
        )



        #Branches tree
        execute(
          sql: %{
            CREATE TABLE IF NOT EXISTS #{name}_branches(
              id UUID NOT NULL,
              parent UUID NOT NULL,
              created date DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY(id, parent)
            )
          }
        )

        execute(
            sql: %{
            CREATE UNIQUE INDEX IF NOT EXISTS
              #{name}_branches_created_idx
            ON
              #{name}_branches(
              created,
              parent,
              id
            )
          }
        )




        execute(
          sql:%{
            CREATE TABLE IF NOT EXISTS
              #{name}_deleted(
                id UUID NOT NULL,
                branch_id UUID NOT NULL,
                deleted DATE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(id)
            )
          },
          **c_
        )





        execute(
          sql: "
            CREATE VIEW IF NOT EXISTS
              #{name} AS
            SELECT
              *
            FROM
              #{name}_src dss
            WHERE
              NOT
                EXISTS (
                  SELECT 1
                  FROM
                    #{name}_deleted dsd
                  WHERE
                    dsd.id == dss.id AND
                    dsd.branch_id = dss.branch_id) AND
                    dss.created = (SELECT MAX(created) FROM #{name}_src WHERE id = dss.id AND branch_id = dss.branch_id)",
          **c_
        )

        #Push INSERT into src table
        execute(
            sql: %{
            CREATE TRIGGER IF NOT EXISTS
              #{name}_uuid_trigger1
            INSTEAD OF
              INSERT
            ON
              #{name}
            WHEN
            NOT
              EXISTS(
                SELECT 1
                FROM
                  #{name}_deleted del
                WHERE
                  del.id = NEW.id AND
                  del.branch_id = NEW.branch_id)
            BEGIN
              INSERT INTO
                #{name}_src(
                  id,
                  branch_id,
                  key,
                  value)
                SELECT
                  COALESCE(NEW.id, UUID()),
                  NEW.branch_id,
                  NEW.key,
                  NEW.value;
            END
          },
            **c_
        )

        #Counterpart to uuid_trigger1: raise error if we try to insert when a value has been deleted
        execute(
            sql: %{
            CREATE TRIGGER IF NOT EXISTS
              #{name}_uuid_trigger2
            INSTEAD OF
              INSERT
            ON
              #{name}
            WHEN
              EXISTS(
                SELECT 1
                FROM
                  #{name}_deleted del
                WHERE
                  del.id = NEW.id AND
                  del.branch_id = NEW.branch_id)
            BEGIN
              SELECT RAISE(FAIL, 'Deleted');
            END
          },
            **c_
        )

        execute(
          sql: %{
            CREATE TRIGGER IF NOT EXISTS
              #{name}_delete_trigger1
            INSTEAD OF
              DELETE
            ON
              #{name}
            WHEN
            NOT
              EXISTS(
                SELECT 1
                FROM
                  #{name}_deleted del
                WHERE
                  del.id = OLD.id AND
                  del.branch_id = OLD.branch_id)
            BEGIN
              INSERT INTO
                #{name}_deleted(
                  id,
                  branch_id
                )
                SELECT
                  OLD.id,
                  OLD.branch_id;
            END
          },
          **c_
        )

        #When we delete, we delete a bunch of records at once. Ignore delete after we create the tombstone
        execute(
          sql: %{
            CREATE TRIGGER IF NOT EXISTS
              #{name}_delete_trigger2
            INSTEAD OF
              DELETE
            ON
              #{name}
            WHEN
              EXISTS(
                SELECT 1
                FROM
                  #{name}_deleted del
                WHERE
                  del.id = OLD.id AND
                  del.branch_id = OLD.branch_id)
            BEGIN
              SELECT NULL;
            END
          },
          **c_
        )
      end
    end
  end
end

Frest::Setup::setup