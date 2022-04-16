import bth.cloud_connector as cloud
import bth.db_connector as db


def iniciar_processo_busca(params_exec, *args, **kwargs):
    # Execução
    url = 'https://contabilidade-fontes-dados.cloud.betha.com.br/contabilidade/fontes-dados/contabil/despesas-extras'

    campos = 'id, exercicio.ano, tipo'
    criterio = f"entidade.id = {db.busca_id_entidade_migracao(params_exec)} and exercicio.ano = {params_exec['exercicio']} and tipo = 'TRANSFERENCIA_FINANCEIRA'"
    cont = 0
    for x in cloud.buscaFonte(url=url, fields=campos, criterio=criterio, token=params_exec['token'], params_exec= params_exec):
        cont+= 1
        print(x)
    print(cont)
    #         url1 = 'https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/despesas-extras'
    #         data = {"idIntegracao": '321231', "idGerado": {"id": x.get('idGerado').get('id')},"content": {"exercicio": exercicio}}
    #         print(data)
    #         data = js.dumps(data)
    #         req1 = requests.delete(url1, headers=header,data=data)
    #         print(req1.json())