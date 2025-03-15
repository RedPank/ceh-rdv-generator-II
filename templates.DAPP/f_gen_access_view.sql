select etl.f_gen_access_view(
p_table_name_list:=array[
('{{ ctx.table_name }}','{{ ctx.distributed_by }}')::etl.tp_table_cols
],
p_schema_name:= '{{ ctx.schema }}',
p_sql_gen_flg:= true,
p_filter_cls:= null,
p_drop_expr_flg:=true
);


