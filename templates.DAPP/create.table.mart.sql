-- Created {{ ctx.created }}
-- В рамках разработки потока {{ ctx.flow_name }}
-- drop table if exists {{ tgt.schema }}.{{ tgt.table_name }} cascade;
CREATE TABLE {{ tgt.schema }}.{{ tgt.table_name }} (
{%- for field in tgt.fields %}
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
DISTRIBUTED BY ({{ tgt.distributed_by }});

{% if tgt.comment|length -%}
COMMENT ON TABLE {{ tgt.schema }}.{{ tgt.table_name }} IS '{{tgt.comment}}';
{% endif -%}

{%- for field in tgt.fields %}
{%- if field.comment|length %}
COMMENT ON COLUMN {{ tgt.schema }}.{{ tgt.table_name }}.{{ field.name }} IS '{{ field.comment }}';
{%- endif %}
{%- endfor -%}