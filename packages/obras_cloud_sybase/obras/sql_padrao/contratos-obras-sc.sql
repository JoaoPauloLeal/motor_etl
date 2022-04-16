SELECT distinct id = 1,
	   os.i_entidades,
	   cmr.id_gerado as i_obras ,
	   cast ((select first mesano from sapo.obras_sit where i_entidades = os.i_entidades and i_obras = os.i_obras) as varchar) as data_inclusao,
       (if exists(select 1 from sapo.contratos where i_entidades = os.i_entidades and numero_contrato = os.contrato and contrato_sup is not null) then
        cast((select first contrato_sup from sapo.contratos where i_entidades = os.i_entidades and numero_contrato = os.contrato and contrato_sup is not null) as varchar)
        else
	    cast(charindex('TAC',os.contrato) as varchar)
        endif) as ee,
	   (if charindex('TAC',ee) > 0 then
	   SUBSTRING(ee,charindex('TAC',ee)+3)
	   else
	   SUBSTRING(ee,charindex('TAC',ee))
	   endif) as nc,
	   charindex('-',nc) as aa,
	   SUBSTRING(nc,aa+1) as numero_contrato,
	   crto.contratado
FROM sapo.obras_sit os 
JOIN bethadba.controle_migracao_registro cmr on (cmr.i_chave_dsk2 = os.i_obras and i_chave_dsk1 = os.i_entidades and cmr.tipo_registro = 'obras')
JOIN sapo.sapo.contratos crto on (crto.i_entidades = os.i_entidades and crto.numero_contrato = os.contrato)
where contrato is not null and charindex('/',numero_contrato) != 0
and contrato != '0' and os.i_entidades = {{entidade}} and not EXISTS
(select 1 from bethadba.controle_migracao_registro e where e.tipo_registro = 'contratos-obras' and e.i_chave_dsk1 = {{entidade}} and e.i_chave_dsk3 = numero_contrato and i_obras = e.i_chave_dsk2 )--and e.i_chave_dsk4 = data_inclusao
order by i_obras desc