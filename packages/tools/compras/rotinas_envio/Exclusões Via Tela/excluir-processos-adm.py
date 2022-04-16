import bth.cloud_connector as cloud

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):

    for x in cloud.buscaFonte(url='https://compras.betha.cloud/compras/dados/api/processosadministrativos',
                              criterio='entidade.id=775',fields='parametroExercicio.exercicio,id',
                              token='92f40c1e-f70f-4cad-a86e-a7d2fe09aad6'):
        print(x)
        print(f"https://compras.betha.cloud/compras-services/api/exercicios/{x['parametroExercicio']['exercicio']}/processos-administrativo/{x['id']}")
        cloud.ExcluirServiceLayerSemJson(token='92f40c1e-f70f-4cad-a86e-a7d2fe09aad6',url=f"https://compras.betha.cloud/compras-services/api/exercicios/{x['parametroExercicio']['exercicio']}/processos-administrativo/{x['id']}")
