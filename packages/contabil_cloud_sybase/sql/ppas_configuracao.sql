select ppa as chave_dsk1,
                                anoInicial
                           from tecbth_delivery.configuracao
                          where idPPA is null and
                                anoInicial =  '{{ano_inicial}}'
                      group by ppa,anoInicial order by anoInicial