SELECT
	id = 1,
	opo.i_entidades,
	opo.i_obras,
	opo.i_planilhas,
	CASE
		WHEN opo.tipo_planilha = 1 THEN 'BASE'
	   	WHEN opo.tipo_planilha = 2 THEN 'CONTRATO'
	   	WHEN opo.tipo_planilha = 3 THEN 'ADITIVO'
	END tipo_planilha,
	cast(opo.data_base as varchar),
	opo.valor,
	cast(opo.data_inclusao as varchar),
	cmr.id_gerado as id_responsavel,
	cmrr.id_gerado as id_obra,
	c.numero_contrato,
    cast(i_obras as varchar)+'/'+cast(i_planilhas as varchar) as chave
FROM sapo.obras_planilha_orcamentaria opo
LEFT JOIN sapo.contratos c ON (opo.i_contratos = c.i_contratos)
LEFT JOIN sapo.responsaveis resp ON (opo.i_responsaveis = resp.i_responsaveis)
LEFT JOIN bethadba.controle_migracao_registro cmr ON (resp.cpf = cmr.i_chave_dsk2 AND cmr.tipo_registro='responsaveis')
LEFT JOIN bethadba.controle_migracao_registro cmrr ON (opo.i_obras = cmrr.i_chave_dsk2 AND cmrr.tipo_registro='obras')
where chave not in (select i_chave_dsk2 from bethadba.controle_migracao_registro where tipo_registro = 'orcamentos' and i_chave_dsk2 = chave)