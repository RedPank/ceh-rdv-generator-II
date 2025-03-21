-- drop table if exists {{ ctx.schema }}.{{ ctx.name }} cascade;
CREATE TABLE {{ ctx.schema }}.{{ ctx.table_name }} (
{%- for field in ctx.fields %}
  {{ field.name }} {{ field.data_type }}
  {%- if not field.is_nullable -%} 
{{ ' not null' }} 
  {%- endif -%}
{%- if not loop.last -%},{% endif -%}
{%- endfor %}
)
WITH (
  appendoptimized=true,
  orientation=column,
  compresslevel=1,
  compresstype=zstd
)
DISTRIBUTED BY ({{ ctx.distributed_by }});

{% if ctx.comment|length -%}
COMMENT ON TABLE {{ ctx.schema }}.{{ ctx.table_name }} IS '{{ctx.comment}}';
{% endif -%}

{%- for field in ctx.fields %}
{%- if field.comment|length %}
COMMENT ON COLUMN {{ ctx.schema }}.{{ ctx.table_name }}.{{ field.name }} IS '{{ field.comment }}';
{%- endif %}
{%- endfor -%}