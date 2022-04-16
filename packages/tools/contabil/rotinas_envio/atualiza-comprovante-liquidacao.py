import time

import bth.interacao_cloud as interacao_cloud
import bth.cloud_connector as cloud
import bth.db_connector as db
import requests as req
import json
import logging
import asyncio
import requests
from datetime import datetime

tipo_registro = 'arrecadacoesOrcamentarias'
sistema = 1
limite_lote = 1000
# PRODUCAO



def iniciar_processo_busca(params_exec, *args, **kwargs):
    print('- Iniciando busca dos dados de dados.')
    lista_conteudo_retorno = []
    lista_dados_enviar = []
    hoje = datetime.now()
    contador = 0

    sql_comprovantes_liq = """
    select       chave_dsk1,
                 chave_dsk2,
                 chave_dsk3,
                 chave_dsk4,
                 chave_dsk5,
                 chave_dsk6,                              
                 exercicio,
                 valor = sum(valor),
                 idCadastro = list(distinct idCadastro order by idCadastro),
                 isNull(bethadba.dbf_get_id_gerado(1,'liquidacoes',chave_dsk1,chave_dsk2,chave_dsk3,idCadastro,exercicio,'LIQ'), '') as idLiquidacao,
                 tipoDoctoFiscal,
                 comprovante = (
                 SELECT list(string('{ "comprovante" : { "id": ', idComprovante, '}, "valor" : ', valor, ' }')) from (
                 	select chavDsk1Comprovante = dctos_fiscais.ano_exerc,
                            chavDsk2Comprovante = dctos_fiscais.i_entidades,
                            chavDsk3Comprovante = dctos_fiscais.i_tipo_dcto_fiscal,
                            chavDsk4Comprovante = dctos_fiscais.i_numero_dcto_fiscal,
                            chavDsk5Comprovante = if dctos_fiscais.tipo_juridico = 'J' then dctos_fiscais.cgc else dctos_fiscais.cpf endif,
                            chavDsk6Comprovante = dctos_fiscais.num_docto,
                            chavDsk7Comprovante = dctos_fiscais.tipo_docto,
                            idComprovante = isNull(bethadba.dbf_get_id_gerado(1,'comprovantes',chavDsk1Comprovante,chavDsk2Comprovante,chavDsk3Comprovante,chavDsk4Comprovante,chavDsk5Comprovante,chavDsk6Comprovante,chavDsk7Comprovante), ''),
                            valor = dctos_fiscais.valor_dcto_fiscal,
                            data = dctos_fiscais.data_emissao,
                            'Liquidação '||liquidacao.i_liquidacao as documento
                       from sapo.dctos_fiscais
                            join sapo.liquidacao on (dctos_fiscais.ano_exerc = liquidacao.ano_exerc and
                                                     dctos_fiscais.i_entidades = liquidacao.i_entidades and
                                                     dctos_fiscais.num_docto = liquidacao.i_liquidacao and
                                                     dctos_fiscais.tipo_docto = 'L')
                      where dctos_fiscais.ano_exerc = 2021 and
                            dctos_fiscais.i_entidades = 4 and
                            dctos_fiscais.tipo_docto = tipoDoctoFiscal and
                            dctos_fiscais.num_docto in ( idCadastro)
                            
                      union all
                      
                     select chavDsk1Comprovante = dctos_fiscais.ano_exerc,
                            chavDsk2Comprovante = dctos_fiscais.i_entidades,
                            chavDsk3Comprovante = dctos_fiscais.i_tipo_dcto_fiscal,
                            chavDsk4Comprovante = dctos_fiscais.i_numero_dcto_fiscal,
                            chavDsk5Comprovante = if dctos_fiscais.tipo_juridico = 'J' then dctos_fiscais.cgc else dctos_fiscais.cpf endif,
                            chavDsk6Comprovante = dctos_fiscais.num_docto,
                            chavDsk7Comprovante = dctos_fiscais.tipo_docto,
                            idComprovante = isNull(bethadba.dbf_get_id_gerado(1,'comprovantes',chavDsk1Comprovante,chavDsk2Comprovante,chavDsk3Comprovante,chavDsk4Comprovante,chavDsk5Comprovante,chavDsk6Comprovante,chavDsk7Comprovante), ''),
                            valor = dctos_fiscais.valor_dcto_fiscal,
                            data = dctos_fiscais.data_emissao,
                            'Liquidação Anterior '||liquidacao_ant.i_liquidacao_ant as documento
                       from sapo.dctos_fiscais
                            join sapo.liquidacao_ant on (dctos_fiscais.ano_exerc = liquidacao_ant.ano_exerc and
                                                         dctos_fiscais.i_entidades = liquidacao_ant.i_entidades and
                                                         dctos_fiscais.num_docto = liquidacao_ant.i_liquidacao_ant and
                                                         dctos_fiscais.tipo_docto = 'A')
                      where dctos_fiscais.ano_exerc = 2021 and
                            dctos_fiscais.i_entidades = 4 and
                            dctos_fiscais.tipo_docto = tipoDoctoFiscal and
                            dctos_fiscais.num_docto in ( idCadastro)
                            
                      union all
                      
                     select chavDsk1Comprovante = dctos_fiscais.ano_exerc,
                            chavDsk2Comprovante = dctos_fiscais.i_entidades,
                            chavDsk3Comprovante = dctos_fiscais.i_tipo_dcto_fiscal,
                            chavDsk4Comprovante = dctos_fiscais.i_numero_dcto_fiscal,
                            chavDsk5Comprovante = if dctos_fiscais.tipo_juridico = 'J' then dctos_fiscais.cgc else dctos_fiscais.cpf endif,
                            chavDsk6Comprovante = dctos_fiscais.num_docto,
                            chavDsk7Comprovante = dctos_fiscais.tipo_docto,
                            idComprovante = isNull(bethadba.dbf_get_id_gerado(1,'comprovantes',chavDsk1Comprovante,chavDsk2Comprovante,chavDsk3Comprovante,chavDsk4Comprovante,chavDsk5Comprovante,chavDsk6Comprovante,chavDsk7Comprovante), ''),
                            valor = dctos_fiscais.valor_dcto_fiscal,
                            data = dctos_fiscais.data_emissao,
                            'Ordem Anterior '||ordens_ant.i_ordens_ant as documento
                       from sapo.dctos_fiscais
                            join sapo.ordens_ant on (dctos_fiscais.ano_exerc = ordens_ant.ano_exerc and
                                                     dctos_fiscais.i_entidades = ordens_ant.i_entidades and
                                                     dctos_fiscais.num_docto = ordens_ant.i_ordens_ant and
                                                     dctos_fiscais.tipo_docto = 'B')
                      where dctos_fiscais.ano_exerc = 2021 and
                            dctos_fiscais.i_entidades = 4 and
                            dctos_fiscais.tipo_docto = tipoDoctoFiscal and
                            dctos_fiscais.num_docto in ( idCadastro)
                            
                      union all
                      
                     select chavDsk1Comprovante = dctos_fiscais.ano_exerc,
                            chavDsk2Comprovante = dctos_fiscais.i_entidades,
                            chavDsk3Comprovante = dctos_fiscais.i_tipo_dcto_fiscal,
                            chavDsk4Comprovante = dctos_fiscais.i_numero_dcto_fiscal,
                            chavDsk5Comprovante = if dctos_fiscais.tipo_juridico = 'J' then dctos_fiscais.cgc else dctos_fiscais.cpf endif,
                            chavDsk6Comprovante = dctos_fiscais.num_docto,
                            chavDsk7Comprovante = dctos_fiscais.tipo_docto,
                            idComprovante = isNull(bethadba.dbf_get_id_gerado(1,'comprovantes',chavDsk1Comprovante,chavDsk2Comprovante,chavDsk3Comprovante,chavDsk4Comprovante,chavDsk5Comprovante,chavDsk6Comprovante,chavDsk7Comprovante), ''),
                            valor = dctos_fiscais.valor_dcto_fiscal,
                            data = dctos_fiscais.data_emissao,
                            'Empenho Anterior '||empenhos_ant.i_empenhos_ant as documento
                       from sapo.dctos_fiscais
                            join sapo.empenhos_ant on (dctos_fiscais.ano_exerc = empenhos_ant.ano_exerc and
                                                       dctos_fiscais.i_entidades = empenhos_ant.i_entidades and
                                                       dctos_fiscais.num_docto = empenhos_ant.i_empenhos_ant and
                                                       dctos_fiscais.tipo_docto = 'E')
                      where dctos_fiscais.ano_exerc = 2021 and
                            dctos_fiscais.i_entidades = 4 and
                            dctos_fiscais.tipo_docto = tipoDoctoFiscal and
                            dctos_fiscais.num_docto in ( idCadastro) and
                            empenhos_ant.liquidado_ano_ant > 0 
                 ) as tab
                 )
        
            from (
                    select liquidacao.i_entidades as chave_dsk1,
                           liquidacao.ano_exerc as chave_dsk2,
                           liquidacao.i_empenhos as chave_dsk3,
                           
                           liquidacao.i_liquidacao as chave_dsk4,
                           liquidacao.ano_exerc as chave_dsk5,
                           'LIQ' as chave_dsk6,
                           
                           liquidacao.i_liquidacao as numeroCadastro,
                           empenhos.data_emissao as emissaoEmp,
                           liquidacao.ano_exerc as exercicio,
                           exercicio as exercEmpenho,
                           liquidacao.ano_exerc as exercParaLiquidar,
                           liquidacao.i_liquidacao as codLiquidacao,
                           liquidacao.i_empenhos as empenho,
                           ISNULL(liquidacao.i_emliquidacao,'') as emLiquidacao,
                           'EL' as tipoEmLiq,
                           date(ISNULL(liquidacao.data_liquidacao,'1900-01-01')) as data,
                           liquidacao.valor as valor,
                           isnull(liquidacao.historico, 'Pela liquidação do empenho '||chave_dsk3) as especificacao,
                           liquidacao.i_liquidacao as idCadastro,
                           'L' as tipoDoctoFiscal,
                           isNull(bethadba.dbf_get_id_gerado(1,'empenhos',chave_dsk1,chave_dsk2,chave_dsk3), '') as idEmpenho,
                           isNull(bethadba.dbf_get_id_gerado(1,'emLiquidacoes',chave_dsk1,chave_dsk2,chave_dsk3,liquidacao.i_emliquidacao,liquidacao.ano_exerc,'EMLIQ'), '') as idEmLiquidacao, 
                           isNull(bethadba.dbf_get_id_gerado(1,'subempenhos',chave_dsk1,chave_dsk2,chave_dsk3, liquidacao.i_subempenhos), '') as idSubEmpenho
                      from sapo.liquidacao join
                           sapo.empenhos on (liquidacao.i_entidades = empenhos.i_entidades and
                                             liquidacao.ano_exerc = empenhos.ano_exerc and
                                             liquidacao.i_empenhos = empenhos.i_empenhos)
                     where liquidacao.i_entidades = 4 and
                           liquidacao.ano_exerc = 2021
                
                     union all
                
                    select liquidacao_ant.i_entidades as chave_dsk1,
                           isnull(empenhos_ant.ano_desp,year(empenhos_ant.data_emissao)) as chave_dsk2,
                           cast(left(liquidacao_ant.i_empenhos_ant, length(liquidacao_ant.i_empenhos_ant) -2) as integer) as chave_dsk3,
                           
                           liquidacao_ant.i_liquidacao_ant as chave_dsk4,
                           liquidacao_ant.ano_exerc as chave_dsk5,
                           'LIQANT' as chave_dsk6,
                           
                           (100000 + liquidacao_ant.i_liquidacao_ant) as numeroCadastro,
                           empenhos_ant.data_emissao as emissaoEmp,
                           liquidacao_ant.ano_exerc as exercicio,
                           empenhos_ant.ano_desp as exercEmpenho,
                           liquidacao_ant.ano_exerc as exercParaLiquidar,
                           liquidacao_ant.i_liquidacao_ant as codLiquidacao,
                           cast(left(liquidacao_ant.i_empenhos_ant, length(liquidacao_ant.i_empenhos_ant) -2) as integer) as empenho,
                           isnull(liquidacao_ant.i_emliquidacao_ant,'') as emLiquidacao,
                           'ELA' as tipoEmLiq,
                           date(ISNULL(liquidacao_ant.data_liquidacao,'1900-01-01')) as data,
                           liquidacao_ant.valor as valor,
                           isnull(liquidacao_ant.historico, 'Pela liquidação do empenho '||chave_dsk3) as especificacao,
                           liquidacao_ant.i_liquidacao_ant as idCadastro,
                           'A' as tipoDoctoFiscal,
                           isNull(bethadba.dbf_get_id_gerado(1,'empenhos',chave_dsk1,chave_dsk2,chave_dsk3), '') as idEmpenho,
                           isNull(bethadba.dbf_get_id_gerado(1,'emLiquidacoes',chave_dsk1,chave_dsk2,chave_dsk3,liquidacao_ant.i_emliquidacao_ant,liquidacao_ant.ano_exerc,'EMLIQANT'), '') as idEmLiquidacao, 
                           '' as idSubEmpenho
                      from sapo.liquidacao_ant join
                           sapo.empenhos_ant on (liquidacao_ant.ano_exerc = empenhos_ant.ano_exerc and
                                                 liquidacao_ant.i_entidades = empenhos_ant.i_entidades and
                                                 liquidacao_ant.i_empenhos_ant = empenhos_ant.i_empenhos_ant)
                     where liquidacao_ant.i_entidades = 4 and
                           liquidacao_ant.ano_exerc = 2021
                
                     union all
                
                    select ordens_ant.i_entidades as chave_dsk1,
                           isnull(ordens_ant.ano_desp,year(ordens_ant.data_emp_origem)) as chave_dsk2,
                           ordens_ant.cod_empenhos as chave_dsk3, 
                           
                           ordens_ant.i_ordens_ant as chave_dsk4,
                           ordens_ant.ano_exerc as chave_dsk5,
                           'OPANT' as chave_dsk6,
                           (300000 + ordens_ant.i_ordens_ant) as numeroCadastro, 
                           ordens_ant.data_emp_origem as emissaoEmp,
                           isnull(ordens_ant.ano_liquidacao,ordens_ant.ano_desp) as exercicio,
                           ordens_ant.ano_desp as exercEmpenho,
                           ordens_ant.ano_desp as exercParaLiquidar,
                           isnull(ordens_ant.liquidacao,0) as codLiquidacao,
                           cod_empenhos as empenho,
                           null as emLiquidacao,
                           null as tipoEmLiq,
                           date(ISNULL((select liquidacao.data_liquidacao
                              from sapo.liquidacao 
                             where liquidacao.ano_exerc = ordens_ant.ano_desp and 
                                   liquidacao.i_entidades = ordens_ant.i_entidades and 
                                   liquidacao.i_liquidacao = ordens_ant.liquidacao),'1900-01-01')) as data,
                           ordens_ant.valor as valor,
                           isnull(ordens_ant.texto, 'Pela liquidação do empenho '||chave_dsk3) as especificacao,
                           ordens_ant.i_ordens_ant as idCadastro,
                           'B' as tipoDoctoFiscal,
                           isNull(bethadba.dbf_get_id_gerado(1,'empenhos',chave_dsk1,chave_dsk2,chave_dsk3), '') as idEmpenho,
                           '' as idEmLiquidacao, 
                           isNull(bethadba.dbf_get_id_gerado(1,'subempenhos',chave_dsk1,chave_dsk2,chave_dsk3,isnull((select liquidacao.i_subempenhos
                                                                                                                        from sapo.liquidacao 
                                                                                                                       where liquidacao.ano_exerc = ordens_ant.ano_desp and 
                                                                                                                             liquidacao.i_entidades = ordens_ant.i_entidades and 
                                                                                                                             liquidacao.i_liquidacao = ordens_ant.liquidacao),0)), '') as idSubEmpenho 
                      from sapo.ordens_ant 
                     where ordens_ant.i_entidades = 4 and
                           ordens_ant.ano_exerc = 2021
                
                     union all
                
                    select empenhos_ant.i_entidades as chave_dsk1,
                           isnull(empenhos_ant.ano_desp,year(empenhos_ant.data_emissao)) as chave_dsk2,
                           cast(left(empenhos_ant.i_empenhos_ant, length(empenhos_ant.i_empenhos_ant) -2) as integer) as chave_dsk3,
                           empenhos_ant.i_empenhos_ant as chave_dsk4, 
                           empenhos_ant.ano_exerc - 1 as chave_dsk5,
                           'EMPANT' as chave_dsk6,
                           (400000 + empenhos_ant.i_empenhos_ant) as numeroCadastro,
                           empenhos_ant.data_emissao as emissaoEmp,
                           empenhos_ant.ano_desp as exercicio,
                           empenhos_ant.ano_desp as exercEmpenho,
                           (empenhos_ant.ano_exerc-1) as exercParaLiquidar,
                           empenhos_ant.i_empenhos_ant as codLiquidacao,
                           cast(left(empenhos_ant.i_empenhos_ant, length(empenhos_ant.i_empenhos_ant) -2) as integer) as empenho,
                           null as emLiquidacao,
                           null as tipoEmLiq,
                           date((empenhos_ant.ano_exerc-1)||'-12-31') as data,
                           empenhos_ant.liquidado_ano_ant as valor,
                           isnull(empenhos_ant.texto, 'Pela liquidação do empenho '||chave_dsk3) as especificacao,
                           empenhos_ant.i_empenhos_ant as idCadastro,
                           'E' as tipoDoctoFiscal,
                           isNull(bethadba.dbf_get_id_gerado(1,'empenhos',chave_dsk1,chave_dsk2,chave_dsk3), '') as idEmpenho,
                           '' as idEmLiquidacao, 
                           isNull(bethadba.dbf_get_id_gerado(1,'subempenhos',chave_dsk1,chave_dsk2,chave_dsk3,isnull((select max(liquidacao.i_subempenhos)+ 1
                                                                                                                        from sapo.ordens_ant oa
                                                                                                                        join sapo.liquidacao on(liquidacao.ano_exerc = oa.ano_desp and 
                                                                                                                                                liquidacao.i_entidades = oa.i_entidades and 
                                                                                                                                                liquidacao.i_liquidacao = oa.liquidacao)
                                                                                                                       where oa.ano_exerc = 2021
                                                                                                                         and oa.i_entidades = 4
                                                                                                                         and liquidacao.i_subempenhos is not null 
                                                                                                                         and liquidacao.i_entidades = empenhos_ant.i_entidades
                                                                                                                         and liquidacao.ano_empenho = empenhos_ant.ano_desp
                                                                                                                         and liquidacao.i_empenhos  = left(empenhos_ant.i_empenhos_ant,length(empenhos_ant.i_empenhos_ant)-2)),0)), '') as idSubEmpenho 
                          from sapo.empenhos_ant
                     where empenhos_ant.i_entidades = 4 and
                           empenhos_ant.ano_exerc = 2021 and
                           valor > 0
                
                
                  ) as Tab
			where comprovante <> '' and tipoDoctoFiscal <> 'B'
			    
         group by chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5, chave_dsk6,  idEmpenho, idEmLiquidacao, emissaoEmp, exercicio, empenho, exercEmpenho, exercParaLiquidar, codLiquidacao, emLiquidacao, idSubEmpenho, tipoEmLiq, tipoDoctoFiscal, comprovante
         having chave_dsk4 = '1129'
    """

    for li_ret, liq_db in enumerate(db.consulta_sql(sql_comprovantes_liq, params_exec)):
        url_base = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/liquidacoes'
        url = f"{url_base}/{liq_db['idLiquidacao']}"

        print(url)

        headers = {'authorization': f"bearer {params_exec['token']}", 'content-type': 'application/json'}
        liq_sl = interacao_cloud.busca_dados_cloud_unit(params_exec, url=url, tipo_registro=tipo_registro,
                                                tamanho_lote=limite_lote)


        # print(f'{li_ret} - {liq_sl}')

        if liq_sl:
            # print(liq_db.get('comprovante'))
            comprovante = [str(liq_db.get('comprovante'))]
            teste = str(comprovante).replace("'{", "{").replace("}'", "}")
            # print(teste)

            # print(venc[0])


            json_envio = {
                "idIntegracao": f"a1b2c3d4{li_ret}",
                "idGerado": {"id": liq_sl.get('idGerado').get('id')},
                "content": {
                    "validaSaldo": False,
                    "exercicio": 2021,
                    "data": liq_sl.get('content').get('data'),
                    "especificacao": str(liq_sl.get('content').get('especificacao')),
                    "empenho": liq_sl.get('content').get('empenho'),
                    "valor": liq_sl.get('content').get('valor'),
                    "comprovantes": json.loads(teste),
                    "numeroCadastro": liq_sl.get('content').get('numeroCadastro'),



                }
            }

            if len(liq_sl.get('content').get('vencimentos')):
                venc = []
                # print(liq_sl.get('content').get('vencimentos'))
                for item_vencimento in liq_sl.get('content').get('vencimentos'):
                    item_vencimento.popitem()
                    # print(item_vencimento)
                    venc.append(item_vencimento)

                vencimentos = {
                    "vencimentos": venc
                }
                json_envio['content'].update(vencimentos)

            # print(json_envio)

            if len(liq_sl.get('content').get('retencoes')):
                print(liq_sl.get('content').get('retencoes'))
                lista_reten = []
                for posicao, reten in enumerate(liq_sl.get('content').get('retencoes')) :
                    print(f'{posicao} - {reten}')
                    # print(reten.get('recursos'))
                    lista_reten.append({
                          "retencao": {
                                "id": reten.get('retencao').get('id')
                            },
                            "valor": reten.get('valor'),
                            "recursos": [{
                                "recurso": {
                                    "id": reten.get('recursos')[0].get('id')
                                },
                                "valor": reten.get('recursos')[0].get('valor'),
                                "contaBancariaAdministradora": reten.get('recursos')[0].get('contaBancariaAdministradora')

                            }]
                        }
                    )
                print(lista_reten)
                retencoes = {
                    "retencoes": lista_reten
                }

                print(retencoes)
                json_envio['content'].update(retencoes)

            print(json_envio)

            print(f"{li_ret} - {json.dumps(json_envio, ensure_ascii=False)}")


            envio = req.put(url=url_base, headers=headers, data=json.dumps(json_envio))
            print(f"resposta : {envio.ok} - Lote {envio.json().get('idLote')}")




            # ret = req.get(url=f"https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/lotes/{envio.json().get('idLote')}", headers=headers)
            # print(ret.json.get('retorno').get('status'))


