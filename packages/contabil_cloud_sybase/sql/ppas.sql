select ppa as chave_dsk1,
                 anoInicial
            from tecbth_delivery.configuracao
           where idPPA is null
        group by ppa,anoInicial order by anoInicial