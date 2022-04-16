-- CORRIGE TODAS AS COMISSOES QUE ESTAO SEM A DATA DE PUBLICACAO
UPDATE compras.responsaveis
SET data_publ = data_desig
WHERE data_publ is NULL;
