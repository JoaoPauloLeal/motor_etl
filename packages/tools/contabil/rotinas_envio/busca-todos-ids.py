import bth.db_connector as db
import bth.cloud_connector as cloud



def iniciar_processo_busca(params_exec, *args, **kwargs):
    i_entidade = db.busca_id_entidade_migracao(params_exec=params_exec)

    # NaturezaJuridicaSQL
    url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/naturezas-juridicas-ctb'
    for e in cloud.buscaFonte(url=url, token=params_exec['token'], fields='id, numero', criterio=''):
        print(e)

    # Logradouros
    url ='https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/enderecos-logradouros'
    for x in cloud.buscaFonte(url=url,token=params_exec['token'], fields='', criterio=''):
        print(x)