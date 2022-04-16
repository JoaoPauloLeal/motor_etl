import bth.db_connector as db
import bth.cloud_connector as cloud

def iniciar_processo_busca(params_exec, ano, *args, **kwargs):
    entidadedsk = 3706

    url = 'https://compras.betha.cloud/compras/dados/api/processosadministrativosparticipantes'

    criterio = f"entidade.id = {entidadedsk} and processoAdministrativo.numeroProcesso in (82,81,80,61,49,48,46,33,17,14,9,4,2) and processoAdministrativo.parametroExercicio.exercicio = 2016"
    print(criterio)
    retorno = cloud.buscaFonte(url=url,
                               criterio=criterio, fields='id, fornecedor.pessoa.nome, processoAdministrativo.numeroProcesso, processoAdministrativo.parametroExercicio.exercicio, processoAdministrativo.situacao', token=params_exec['token'])

    for x in retorno:
        if x['processoAdministrativo']['situacao'] == 'EM_EDICAO':

            print(x)
        exercicio = x.get('processoAdministrativo').get('parametroExercicio').get('exercicio')
        processoAdministrativoId = x.get('processoAdministrativo').get('id')
        id = x.get('id')
        urlEx = f'https://compras.betha.cloud/compras-services/api/exercicios/{exercicio}/processos-administrativo/{processoAdministrativoId}/participante-licitacao/{id}'
        print(urlEx)
        print(cloud.ExcluirServiceLayerSemJson(url=urlEx, token=params_exec['token']))




