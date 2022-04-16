SELECT
	id = 1,
	oa.i_entidades,
	oa.i_obras,
	oa.i_art as nroResponsabilidadeTecnica,
    cast(oa.i_obras as varchar)+'/'+cast(nroResponsabilidadeTecnica as varchar) as chave,
	cast(substring(ob.data_cadastramento,0,11) as varchar) as data_inclusao,
    r.cpf,
	cmr.id_gerado as responsavel,
	cmrr.id_gerado as tipoResponsabilidade,
	cmrrr.id_gerado as id_obra
FROM sapo.obras_art oa
JOIN sapo.responsaveis r ON (r.i_responsaveis = oa.i_responsaveis)
JOIN sapo.obras ob on (ob.i_obras = oa.i_obras)
INNER JOIN bethadba.controle_migracao_registro cmr ON (cmr.i_chave_dsk2 = r.cpf AND cmr.tipo_registro = 'responsaveis')
INNER JOIN bethadba.controle_migracao_registro cmrr ON (oa.tipo_rt = cmrr.i_chave_dsk2 AND cmrr.tipo_registro = 'tipo-responsabilidade-tecnica')
INNER JOIN bethadba.controle_migracao_registro cmrrr ON (oa.i_obras = cmrrr.i_chave_dsk2 AND cmrrr.tipo_registro = 'obras')
where chave not in (select i_chave_dsk2 from bethadba.controle_migracao_registro where tipo_registro = 'obra-art' and i_chave_dsk2 = chave)
UNION
select id = 1,
	oa.i_entidades,
	oa.i_obras,
    '1' as nroResponsabilidadeTecnica,
    cast(oa.i_obras as varchar)+'/'+cast(nroResponsabilidadeTecnica as varchar) as chave,
    substring(oa.data_cadastramento,0,11) as data_inclusao,
    (select first i_chave_dsk2 from bethadba.controle_migracao_registro where tipo_registro = 'responsaveis') as cpf,
    (select first id_gerado from bethadba.controle_migracao_registro where tipo_registro = 'responsaveis') as responsavel,
    (select first id_gerado from bethadba.controle_migracao_registro where tipo_registro = 'tipo-responsabilidade-tecnica') as tipoResponsabilidade,
    cmrrr.id_gerado as id_obra
from sapo.obras oa
INNER JOIN bethadba.controle_migracao_registro cmrrr ON (oa.i_obras = cmrrr.i_chave_dsk2 AND cmrrr.tipo_registro = 'obras')
where oa.i_obras not in (select i_obras from sapo.obras_art) and chave not in (select i_chave_dsk2 from bethadba.controle_migracao_registro where tipo_registro = 'obra-art' and i_chave_dsk2 = chave)