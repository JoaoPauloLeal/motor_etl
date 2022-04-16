SELECT DISTINCT
	id = 1,
	i_entidades,
	i_obras,
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_obra,
	cast(data_conclusao as varchar) as data_medicao,
	observacao = 'Migração',
	i_medicao_acompanhamento = 'null'
FROM sapo.obras
INNER JOIN bethadba.controle_migracao_registro cmr ON (cmr.tipo_registro = 'responsaveis' and cmr.i_chave_dsk1 in (1,2,3,4,5,6,7,8,9,10))
INNER JOIN bethadba.controle_migracao_registro cmrr ON (i_obras = cmrr.i_chave_dsk2 AND cmrr.tipo_registro='obras' AND cmrr.i_chave_dsk1 = {{entidade}})
WHERE i_entidades = {{entidade}} and data_conclusao is not null and i_obras not in (select substring(i_chave_dsk2,charindex('/',i_chave_dsk2)+1) from bethadba.controle_migracao_registro where tipo_registro = 'conclusoes' and i_chave_dsk1 = {{entidade}})