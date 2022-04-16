import bth.cloud_connector as cloud
import requests

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):

    for x in cloud.buscaFonte(url='https://compras.betha.cloud/compras/dados/api/processosadministrativositens',
                              criterio='entidade.id = 775',fields='id,configuracao.processoAdministrativo.id',
                              token='92f40c1e-f70f-4cad-a86e-a7d2fe09aad6'):
        print(x)
        print(f"https://compras.betha.cloud/compras/api/processosadministrativo/{x['configuracao']['processoAdministrativo']['id']}/itens/itemgerado/{x['id']}")
        header = {'content-type': 'application/json', 'Authorization': f'Bearer 9ddaf153-109c-4457-8369-986bed0779ac',
                  'user-access':'n79Y3rAPc04='}
        req = requests.delete(url=f"https://compras.betha.cloud/compras/api/processosadministrativo/{x['configuracao']['processoAdministrativo']['id']}/itens/itemgerado/{x['id']}",
                            headers=header)
        print(req.status_code)
        # cloud.ExcluirServiceLayerSemJson(token='372ee99c-e86a-48ed-b6e6-8ee80b643175',url=f"https://compras.betha.cloud/compras-services/api/exercicios/{x['parametroExercicio']['exercicio']}/processos-administrativo/{x['id']}")
