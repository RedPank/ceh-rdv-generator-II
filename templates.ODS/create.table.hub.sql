-- Created {{ ctx.created }}
-- В рамках разработки потока {{ ctx.flow_name }}
CREATE TABLE IF NOT EXISTS {{ctx.full_table_name}} (
    {{ctx.rk_field}} int8 NOT NULL,
    {{ctx.id_field}} text NOT NULL,
    src_cd text NOT NULL,
    bk_type text NOT NULL,
    invalid_id int8 NOT NULL,
    version_id int8 NOT NULL
)
WITH (
  appendoptimized=true,
  orientation=column,
  compresslevel=1,
  compresstype=zstd
)
DISTRIBUTED BY ({{ctx.name}});

DO $$

DECLARE
    v_res      numeric;
BEGIN

    SELECT COUNT(*)
      FROM information_schema.tables INTO v_res
     WHERE table_schema = '{{ctx.schema}}'
       AND table_name   = '{{ctx.hub_name_only}}';

    IF v_res > 0 THEN
        EXECUTE 'SELECT COUNT(*) FROM {{ctx.full_table_name}} WHERE {{ ctx.rk_field }} = -1'
            INTO v_res;

        IF v_res = 0 THEN
        EXECUTE 'INSERT INTO {{ctx.full_table_name}} ({{ctx.rk_field}}, {{ctx.id_field}}, src_cd, bk_type, invalid_id, version_id)
                 VALUES (-1, ''~default~novalue~'', ''DEFAULT'', ''DEFAULT_NO_OBJECT'', 0, 0)';

            RAISE notice ' =  =  =  =  = -3.%. record has been added', clock_timestamp();
        ELSE
            RAISE notice ' =  =  =  =  = -3.%. record already exists, step skipped', clock_timestamp();
        END IF;

        EXECUTE 'SELECT COUNT(*) FROM {{ctx.full_table_name}} WHERE {{ctx.rk_field}} = -2'
            INTO v_res;

        IF v_res = 0 THEN
        EXECUTE 'INSERT INTO {{ctx.full_table_name}} ({{ctx.rk_field}}, {{ctx.id_field}}, src_cd, bk_type, invalid_id, version_id)
                 VALUES (-2, ''~default~unknownvalue~'', ''DEFAULT'', ''DEFAULT_UNKNOWN_OBJECT'', 0, 0)';
            RAISE notice ' =  =  =  =  = -3.%. record has been added', clock_timestamp();
        ELSE
            RAISE notice ' =  =  =  =  = -3.%. record already exists, step skipped', clock_timestamp();
        END IF;
    END IF;
END$$;