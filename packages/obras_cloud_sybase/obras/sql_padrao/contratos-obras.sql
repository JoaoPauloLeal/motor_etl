SELECT
    id = 1,
    oc.i_entidades,
    oc.i_obras as obra,
    oc.i_contratos,
    cast(oc.data_inclusao as varchar) data_inclusao,
    oc.i_responsaveis,
    ISNULL(c.contrato_sup, c.numero_contrato) as numero_contrato,
    c.i_contratos as contrato_dsk,
    cmr.id_gerado as i_obras,
    c.contratado
FROM sapo.obras_contratos oc
INNER JOIN sapo.contratos c ON (oc.i_contratos = c.i_contratos)
INNER JOIN bethadba.controle_migracao_registro cmr ON (oc.i_obras = cmr.i_chave_dsk2 and cmr.tipo_registro='obras')
where oc.i_entidades = {{entidade}} and cmr.id_gerado not in (select i_chave_dsk2 from bethadba.controle_migracao_registro where tipo_registro = 'contratos-obras' and
i_chave_dsk2 = cmr.id_gerado and i_chave_dsk1 = {{entidade}})