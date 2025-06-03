-- Created {{ ctx.created }}
select etl.f_gen_access_view(
p_table_name_list:=array[
('{{ tgt.table_name }}','{{ tgt.primary_key }}')::etl.tp_table_cols
],
p_schema_name:= '{{ tgt.schema }}',
p_sql_gen_flg:= true,
p_filter_cls:= null,
p_drop_expr_flg:=true
);


