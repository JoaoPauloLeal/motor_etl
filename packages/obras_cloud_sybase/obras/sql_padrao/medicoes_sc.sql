SELECT
	id = 1,
	TRIM(oma.num_medicao) as i_medicao_acompanhamento,
	isnull(oma.i_entidades,oma.percentual_fisico,null) as percentual_fisico,
	oma.i_obras,
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_tipo_medicao,
	cmrrr.id_gerado as id_obra,
	oma.vl_medicao as valor,
	oma.i_entidades,
	' ' as observacao,
	cast(oma.dt_medicao as varchar) as data_medicao,
	null as numero_contrato FROM sapo.medicao_obras_contrato oma
		JOIN bethadba.controle_migracao_registro cmr ON (cmr.tipo_registro='responsaveis' and cmr.i_chave_dsk1 = 1)
		JOIN bethadba.controle_migracao_registro cmrr ON (cmrr.i_chave_dsk2 = 'M' AND cmrr.tipo_registro='tipo-medicao')
		JOIN bethadba.controle_migracao_registro cmrrr ON (oma.i_obras = cmrrr.i_chave_dsk2 AND cmrrr.tipo_registro='obras' and cmrrr.i_chave_dsk1 = oma.i_entidades)
WHERE i_entidades = {{entidade}}