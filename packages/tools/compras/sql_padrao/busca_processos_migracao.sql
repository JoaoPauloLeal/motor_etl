--BUSCA TODOS OS PROCESSOS A SEREM MIGRADOS ANTES DE REALIZAR A MIGRAÇÃO DOS PROCESSOS
--POR ISSO NÃO POSSUEM OS IDS GERADOS
SELECT 'processos-administrativo' as tipo_registro,
	processos.i_entidades as chave_dsk1,
	processos.i_ano_proc as chave_dsk2,
	processos.i_processo as chave_dsk3
FROM compras.processos
WHERE processos.i_entidades = {{entidade}}
	and processos.i_forma_julg is not null
	and (
		(processos.i_ano_proc >= {{exercicio}})
		or
		(processos.i_ano_proc < {{exercicio}}
			and processos.data_homolog is null
            and not exists(
            	select ap.i_processo
            	from compras.anl_processos ap
            	where ap.i_entidades = processos.i_entidades
            		and ap.i_processo = processos.i_processo
            		and ap.i_ano_proc = processos.i_ano_proc)
         )
         or
         (processos.i_ano_proc < {{exercicio}}
         	and processos.data_homolog is not null
         	and (
         		(processos.data_contrato_transformers is null
         			and processos.continuado_ano >={{exercicio}})
                 or
                 year(processos.data_contrato_transformers) >={{exercicio}})
         )
         )
         and exists(
         	select ano_exerc
         	from compras.parametros_anuais
         	where parametros_anuais.i_entidades = {{entidade}}
         		and parametros_anuais.ano_exerc = processos.i_ano_proc
         		)
 order by 3,4